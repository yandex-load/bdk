import requests
import logging
import simplejson
import subprocess
import time
import sys

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

log = logging.getLogger(__name__)

def handle_job(job):
    params = {
        "meta.upload_token": job.get("upload_token"),
        "meta.jobno": job.get("id"),
    }
    cmd = "yandex-tank --lock-dir . " + " ".join(
        "-o %s=%s" % (k, v) for k, v in params.items()) + \
        " -c https://lunapark.test.yandex-team.ru/api/job/%s/configinitial.txt" % job.get("id")

    log.info("Running Tank: %s", cmd)
    subprocess.call([
        part.decode(sys.getfilesystemencoding()) for part in cmd.split()])

def main():
    while True:
        resp = requests.get("https://lunapark.test.yandex-team.ru/firestarter/claim_job", params=dict(tank="mytank"), verify=False)
        if resp.status_code == 200:
            try:
                data = resp.json()
                if data.get("success") == 1:
                    try:
                        handle_job(data["job"])
                    except KeyError:
                        log.error("Malformed JSON: no job section.\n%s\n", simplejson.dumps(data))
                else:
                    log.error("Error claiming job: %s", data.get("error", simplejson.dumps(data)))
            except simplejson.JSONDecodeError:
                log.exception("Error decoding JSON response:\n%s\n", resp.text)
        elif resp.status_code == 404:
            log.debug(resp.text)
            time.sleep(3)
        else:
            log.error("Non-200 response code: %s", resp.status_code)
            try:
                log.error("\n%s\n", resp.json().get("error", resp.text))
            except simplejson.JSONDecodeError, ValueError:
                log.error(resp.text)

if __name__ == '__main__':
    logging.basicConfig(level="INFO")
    main()
