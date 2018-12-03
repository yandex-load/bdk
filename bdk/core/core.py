import threading
import urlparse
from Queue import Queue, Empty

import requests
import simplejson
import logging
import time
import tempfile
import yaml
import socket

from retrying import retry, RetryError

from bdk.core.config.dynamic_options import DYNAMIC_OPTIONS
from bdk.executors.universal import UniversalExecutor

from netort.validated_config import ValidatedConfig

logger = logging.getLogger(__name__)


RETRY_ARGS = dict(
    wrap_exception=True,
    stop_max_delay=10000,
    wait_fixed=1000,
    stop_max_attempt_number=5
)


class ExecutorFactory(object):
    def __init__(self):
        self.executors = {}  # for future custom executors

    def detect_executor(self, cmd):
        return self.executors[cmd] if cmd in self.executors else UniversalExecutor


class BDKCore(threading.Thread):
    PACKAGE_SCHEMA_PATH = 'bdk.core'
    FMT = 'yaml'

    def __init__(self, config):
        super(BDKCore, self).__init__()
        self.config = ValidatedConfig(config, DYNAMIC_OPTIONS, self.PACKAGE_SCHEMA_PATH)
        self.cmd = self.config.get_option('executable', 'cmd')

        self.api_poll_interval = self.config.get_option('configuration', 'interval')
  # Used by LPQ for internal usage (like datacenter detection etc)
        self.capabilities = self.config.get_option('executable', 'capabilities')
        self.capabilities['__fqdn'] = socket.getfqdn()
        self.myname = self.capabilities.get('host_name', socket.getfqdn())

        self.lpq_client = LPQClient(self.config.get_option('configuration', 'api_address'),)
        self.interrupted = threading.Event()
        self.executor = None

    def configure(self):
        logger.info('Configuring...')
        logger.info('My name: %s', self.myname)

        factory = ExecutorFactory()
        self.executor = factory.detect_executor(self.cmd)(self.config)
        logger.info('Using %s for %s', self.executor, self.cmd)

    def run(self):
        logger.info('Starting...')
        while not self.interrupted.is_set():
            try:
                job = self.lpq_client.claim_task(self.capabilities)
            except RetryError:
                logger.warning('Claim job failed!', exc_info=True)
                time.sleep(self.api_poll_interval)
            else:
                if not job:
                    logging.info("No jobs.")
                    time.sleep(self.api_poll_interval)
                else:
                    logger.info('Task id: %s', job.id)
                    if job.config is not None:
                        with job.stdout_sender() as sender:
                            return_code = self.executor.run(self.__dump_job_config_to_disk(job.config),
                                                            sender)
                        job.finish(return_code)
                    else:
                        logger.info('There is no `job` section in job. Nothing to do...')
                        logger.debug('There is no `job` section in job. Config: %s', job, exc_info=True)
                        continue

    @staticmethod
    def __dump_job_config_to_disk(config_contents):
        conffile = tempfile.mktemp()
        with open(conffile, 'wb') as f:
            f.write(yaml.safe_dump(config_contents))
        return conffile

    def interrupt(self):
        self.interrupted.set()


@retry(**RETRY_ARGS)
def retry_post(url, data=None, json=None, timeout=5, session=None):
    """
    :type session: requests.Session
    """
    if session:
        return session.post(url, data=data, json=json, verify=False, timeout=timeout)
    else:
        return requests.post(url, data=data, json=json, verify=False, timeout=timeout)


class SenderThread(threading.Thread):
    def __init__(self, queue, sender_method):
        """

        :type queue: Queue
        """
        threading.Thread.__init__(self)
        self.queue = queue
        self.sender_method = sender_method
        self.finished = threading.Event()

    def run(self):
        while not self.finished.is_set():
            try:
                data = self.queue.get(timeout=1)
            except Empty:
                continue
            else:
                try:
                    self.sender_method(data)
                except RetryError:
                    logging.error('Failed to upload stdout chunk', exc_info=True)
                    self.interrupt()

    def finish(self):
        self.finished.set()
        while not self.queue.empty():
            data = self.queue.get_nowait()
            try:
                self.sender_method(data)
            except RetryError:
                logging.error('Failed to upload stdout chunk', exc_info=True)
                self.interrupt()
                break

    def interrupt(self):
        self.finished.set()
        while not self.queue.empty():
            self.queue.get_nowait()
        logging.error('Upload interrupted')


class StdoutSender(object):
    def __init__(self, job_id, sender_method, buffer_size=500*1024):
        self.job_id = job_id
        self.sender_method = sender_method
        self.buffer_size_limit = buffer_size
        self.buffer_size = 0
        self.buffer = ''

    def __enter__(self):
        self.queue = Queue()
        self.sender_thread = SenderThread(self.queue, self.sender_method)
        self.sender_thread.start()
        logger.debug("Sender thread for job {} started".format(self.job_id))

        def send_data(stdout_line):
            """
            :type stdout_line: unicode
            """
            result_line = '{}:: {}\n'.format(int(time.time()*1000), stdout_line)  # "ts.ms:: log line"
            if self.buffer_size < self.buffer_size_limit:
                self.buffer += result_line
                self.buffer_size += len(result_line)
            else:
                self._send_buffer()
                self.buffer = result_line
                self.buffer_size = len(result_line)
        return send_data

    def _send_buffer(self):
        self.queue.put(self.buffer.rstrip())
        self._clear_buffer()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            logger.error("There was {} error: {}\n{}".format(exc_type, exc_val, exc_tb))
        self._send_buffer()
        self.sender_thread.finish()
        self.sender_thread.join()
        logger.debug('Sender thread for job {} joined'.format(self.job_id))

    def _clear_buffer(self):
        self.buffer = ''
        self.buffer_size = 0


class LPQClient(object):
    def __init__(self, base_address):
        self.base_address = base_address
        self.claim_url = urlparse.urljoin(base_address, '/claim.yaml')

    def claim_task(self, capabilities):
        """

        :rtype: LPQJob
        """
        try:
            resp = retry_post(self.claim_url, data=yaml.dump({'capabilities': capabilities}))
        except RetryError:
            logger.warning('Max number of retries for claim job exceeded', exc_info=True)
            raise
        except Exception:
            logger.error('Unknown exception! Fixme!', exc_info=True)
            return
        else:
            if resp.status_code == 200:
                logger.debug('Received job: %s', resp.text)
                try:
                    claimed_job = yaml.safe_load(resp.text)
                except simplejson.JSONDecodeError:
                    logger.exception("Error decoding JSON response:\n%s\n", resp.text)
                else:
                    return LPQJob(self, claimed_job)
            elif resp.status_code == 404:
                logger.debug('No jobs from api, 404: %s', resp.text)
            elif resp.status_code == 400:
                logger.exception("Bad request. Response: %s", resp.text)
            else:
                logger.error("Non-200 response code: %s", resp.status_code)
                logger.debug('Failed to claim job: %s', resp.text, exc_info=True)

    def get_send_status(self, job_id):
        endpoint = '/job/{}/finish.json'.format(job_id)
        url = urlparse.urljoin(self.base_address, endpoint)

        def send_status(rc):
            return retry_post(url, json={'return code': rc})

        return send_status

    def get_send_stdout(self, job_id, session):
        endpoint = '/job/{}/stdout'.format(job_id)
        url = urlparse.urljoin(self.base_address, endpoint)

        def send_stdout(content):
            return retry_post(url, data=content, session=session)

        return send_stdout


class LPQJob(object):

    def __init__(self, lpq_client, job_data):
        """

        :type lpq_client: LPQClient
        """
        self.id = job_data.get('task_id')
        self.config = job_data.get('config')
        self.session = requests.Session()
        self.finish = lpq_client.get_send_status(self.id)
        self.send_stdout = lpq_client.get_send_stdout(self.id, session=self.session)

    def stdout_sender(self, buffer_size=500*1024):
        return StdoutSender(self.id, self.send_stdout, buffer_size=buffer_size)
