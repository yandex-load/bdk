import requests
import simplejson
import subprocess
import sys
import logging
import time

from bdk.core.config.dynamic_options import DYNAMIC_OPTIONS
from netort.validated_config import ValidatedConfig

logger = logging.getLogger(__name__)


# curl -X POST -H"Content-Type:text/plain" --data-binary @./test.yaml https://lunapark.yandex-team.ru/api/v2/jobs/?format=yaml


class BDKCore(object):
    PACKAGE_SCHEMA_PATH = 'bdk.core'

    def __init__(self, config):
        self.config = ValidatedConfig(config, DYNAMIC_OPTIONS, self.PACKAGE_SCHEMA_PATH)

        self.myname = self.config.get_option('capabilities', 'host').get('name')

        self.cmd = self.config.get_option('executable', 'cmd')
        self.cmd_params = self.config.get_option('executable', 'params')

        self.api_poll_interval = self.config.get_option('configuration', 'interval')
        self.api_address = self.config.get_option('configuration', 'api_address')
        self.api_handler = self.config.get_option('configuration', 'api_claim_handler')

        self.claim_request = "{api_address}/{api_handler}".format(
            api_address=self.api_address, api_handler=self.api_handler
        )
        self.claim_params = {
            'tank': self.myname
        }

        self.interrupted = False

    def configure(self):
        logger.info('Configuring...')
        logger.info("My name: %s", self.myname)
        logger.debug('Claim backend: %s', self.claim_request)

    def start(self):
        logger.info('Starting...')
        while not self.interrupted:
            job = self.__claim()
            if not job:
                logging.info("No jobs.")
                time.sleep(self.api_poll_interval)
            else:
                try:
                    job_data = job.json()
                    if job_data.get("success"):
                        try:
                            self.__run_job(job_data["job"])
                        except KeyError:
                            logger.error(
                                "Malformed JSON: no job section.\n%s\n",
                                simplejson.dumps(job_data), exc_info=True
                            )
                    else:
                        logger.error(
                            "Error claiming job: %s",
                            job_data.get("error", simplejson.dumps(job_data)))
                except simplejson.JSONDecodeError:
                    logger.exception("Error decoding JSON response:\n%s\n", job.text)

    def stop(self):
        logger.info('Got interrupt signal!')
        self.interrupted = True

    def __claim(self):
        try:
            resp = requests.get(self.claim_request, params=self.claim_params, verify=False)
        except requests.ConnectionError, requests.ConnectTimeout:
            logger.exception('Connection error, retrying in %s...', self.api_poll_interval, exc_info=True)
            return
        except Exception:
            logger.error('Unknown exception! Fixme!', exc_info=True)
            return
        else:
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                logger.debug('No jobs from api, 404: %s', resp.text)
            elif resp.status_code == 400:
                logger.exception("Bad request. Response: %s", resp.text)
            else:
                logger.error("Non-200 response code: %s", resp.status_code)
                logger.debug('Failed to claim job: %s', resp.text, exc_info=True)

    def __run_job(self, job):
        for item in job:
            logger.info('Item: %s', item)
        params = {
            "meta.upload_token": job.get("upload_token"),
            "meta.jobno": job.get("id"),
        }
        cmdline = self.cmd
        #+ " " + \
        #    ("--lock-dir /tmp " if self.config.get_option('core', 'darwin') + \
        #    " ".join("-o %s=%s" % (k, v) for k, v in params.items()) + \
        #    " -c %s/api/job/%s/configinitial.txt" % (self.api, job.get("id"))

        logger.info("Running Task: %s", cmdline)
        subprocess.call([
            part.decode(sys.getfilesystemencoding())
            for part in cmdline.split()])
