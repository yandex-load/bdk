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


@retry(**RETRY_ARGS)
def send_chunk(url, data, timeout=5):
    r = requests.post(url, data=data, verify=False, timeout=timeout)
    return r


class ExecutorFactory(object):
    def __init__(self):
        self.executors = {}  # for future custom executors

    def detect_executor(self, cmd):
        return self.executors[cmd] if cmd in self.executors else UniversalExecutor


class BDKCore(object):
    PACKAGE_SCHEMA_PATH = 'bdk.core'
    FMT = 'yaml'

    def __init__(self, config):
        self.config = ValidatedConfig(config, DYNAMIC_OPTIONS, self.PACKAGE_SCHEMA_PATH)
        self.cmd = self.config.get_option('executable', 'cmd')

        self.api_poll_interval = self.config.get_option('configuration', 'interval')
        self.api_address = self.config.get_option('configuration', 'api_address')
        self.api_handler = self.config.get_option('configuration', 'api_claim_handler')

        self.capabilities = self.config.get_option('executable', 'capabilities')
        self.myname = self.capabilities.get('host_name', socket.getfqdn())
        self.set_default_capabilities()
        self.capabilities = yaml.dump({'capabilities': self.capabilities})

        self.claim_request = "{api_address}{api_handler}".format(
            api_address=self.api_address, api_handler=self.api_handler
        )
        self.claim_data = self.capabilities

        self.interrupted = False
        self.executor = None

    def set_default_capabilities(self):
        self.capabilities['__fqdn'] = socket.getfqdn()  # Used by LPQ for internal usage (like datacenter detection etc)

    def configure(self):
        logger.info('Configuring...')
        logger.info('My name: %s', self.myname)

        factory = ExecutorFactory()
        self.executor = factory.detect_executor(self.cmd)(self.config)
        logger.info('Using %s for %s', self.executor, self.cmd)

    def start(self):
        logger.info('Starting...')
        while True:
            try:
                job = self.__claim()
            except RetryError:
                logger.warning('Claim job failed!', exc_info=True)
                time.sleep(self.api_poll_interval)
            else:
                if not job:
                    logging.info("No jobs.")
                    time.sleep(self.api_poll_interval)
                else:
                    logger.info('Task id: %s', job.get('task_id'))
                    if job.get('config'):
                        self.executor.run(
                            self.__dump_job_config_to_disk(job.get('config'))
                        )
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

    def __claim(self):
        try:
            resp = send_chunk(url=self.claim_request, data=self.claim_data)
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
                    return claimed_job
            elif resp.status_code == 404:
                logger.debug('No jobs from api, 404: %s', resp.text)
            elif resp.status_code == 400:
                logger.exception("Bad request. Response: %s", resp.text)
            else:
                logger.error("Non-200 response code: %s", resp.status_code)
                logger.debug('Failed to claim job: %s', resp.text, exc_info=True)
