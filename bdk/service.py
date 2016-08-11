import requests
import logging
import simplejson
import subprocess
import time
import argparse
import sys

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

log = logging.getLogger(__name__)


class TankManager(object):
    def __init__(self, api):
        self.api = api

    def claim(self):
        resp = requests.get(
            self.api+"/firestarter/claim_job",
            params=dict(tank="mytank"), verify=False)
        if resp.status_code == 200:
            try:
                data = resp.json()
                if data.get("success") == 1:
                    try:
                        self.run_job(data["job"])
                    except KeyError:
                        log.error(
                            "Malformed JSON: no job section.\n%s\n",
                            simplejson.dumps(data))
                else:
                    log.error(
                        "Error claiming job: %s",
                        data.get("error", simplejson.dumps(data)))
            except simplejson.JSONDecodeError:
                log.exception("Error decoding JSON response:\n%s\n", resp.text)
        elif resp.status_code == 404:
            log.debug(resp.text)
        else:
            log.error("Non-200 response code: %s", resp.status_code)
            try:
                log.error("\n%s\n", resp.json().get("error", resp.text))
            except simplejson.JSONDecodeError:
                log.error(resp.text)
        return resp.status_code

    def run_job(self, job):
        params = {
            "meta.upload_token": job.get("upload_token"),
            "meta.jobno": job.get("id"),
        }
        cmd = "yandex-tank --lock-dir . " + " ".join(
            "-o %s=%s" % (k, v) for k, v in params.items()) + \
            " -c %s/api/job/%s/configinitial.txt" % (self.api, job.get("id"))

        log.info("Running Tank: %s", cmd)
        subprocess.call([
            part.decode(sys.getfilesystemencoding()) for part in cmd.split()])


def main():
    parser = argparse.ArgumentParser(description='Process jobs from Tank task queue.')
    parser.add_argument(
        '-e', '--endpoint',
        default='https://lunapark.yandex-team.ru',
        help='job queue server address')

    args = parser.parse_args()
    tm = TankManager(args.endpoint.strip("/"))
    while True:
        if tm.claim() == 404:
            time.sleep(3)

if __name__ == '__main__':
    logging.basicConfig(level="INFO")
    main()
