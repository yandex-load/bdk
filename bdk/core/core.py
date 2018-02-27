import requests
import simplejson
import logging
import time
import tempfile
import yaml

from bdk.core.config.dynamic_options import DYNAMIC_OPTIONS
from bdk.executors.universal import UniversalExecutor

from netort.validated_config import ValidatedConfig

logger = logging.getLogger(__name__)


# curl -X POST -H"Content-Type:text/plain" --data-binary @./bdk.yaml https://lunapark.yandex-team.ru/api/v2/jobs/?format=yaml


class ExecutorFactory(object):
    def __init__(self):
        self.executors = {}  # for future custom executors

    def detect_executor(self, cmd):
        return self.executors[cmd] if cmd in self.executors else UniversalExecutor


class BDKCore(object):
    PACKAGE_SCHEMA_PATH = 'bdk.core'

    def __init__(self, config):
        self.config = ValidatedConfig(config, DYNAMIC_OPTIONS, self.PACKAGE_SCHEMA_PATH)

        self.myname = self.config.get_option('capabilities', 'host').get('name')

        self.cmd = self.config.get_option('executable', 'cmd')

        self.api_poll_interval = self.config.get_option('configuration', 'interval')
        self.api_address = self.config.get_option('configuration', 'api_address')
        self.api_handler = self.config.get_option('configuration', 'api_claim_handler')

        self.claim_request = "{api_address}/{api_handler}?tank={myname}".format(
            api_address=self.api_address, api_handler=self.api_handler, myname=self.myname
        )

        self.interrupted = False
        self.executor = None

    def configure(self):
        logger.info('Configuring...')
        logger.info("My name: %s", self.myname)
        logger.debug('Claim backend: %s', self.claim_request)

        factory = ExecutorFactory()
        self.executor = factory.detect_executor(self.cmd)(self.config)
        logger.info('Using %s for %s', self.executor, self.cmd)

    def start(self):
        logger.info('Starting...')
        while True:
            job = self.__claim()
            if not job:
                logging.info("No jobs.")
                time.sleep(self.api_poll_interval)
            else:
                if job.get('success'):
                    if job.get('job'):
                        self.executor.run(
                            self.__dump_job_config_to_disk(
                                job.get('job')
                            )
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
            resp = requests.get(self.claim_request, verify=False)
        except requests.ConnectionError, requests.ConnectTimeout:
            logger.exception('Connection error, retrying in %s...', self.api_poll_interval, exc_info=True)
            return
        except Exception:
            logger.error('Unknown exception! Fixme!', exc_info=True)
            return
        else:
            if resp.status_code == 200:
                try:
                    claimed_job = resp.json()
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
