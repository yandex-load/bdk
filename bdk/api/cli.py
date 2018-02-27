import logging
import argparse
import yaml

from bdk.core.core import BDKCore
from netort.logging_and_signals import init_logging, set_sig_handler

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Process jobs from Task task queue.')
    parser.add_argument('-q', '--quiet', action='store_true', dest='quiet', default=False)
    parser.add_argument('-d', '--debug', '--verbose', action='store_true', dest='verbose', default=False)
    parser.add_argument('-c', '--config', dest='config', default='/etc/bdk.yaml')
    parser.add_argument('-l', '--log', dest='log', default='bdk.log')
    args = parser.parse_args()

    init_logging(args.log, args.verbose, args.quiet)
    if not args.verbose:
        logging.getLogger("requests").setLevel("ERROR")

    cfg_dict = {}
    with open(args.config, 'r') as cfg_stream:
        try:
            cfg_dict = yaml.safe_load(cfg_stream)
        except yaml.YAMLError:
            logger.debug('Config file format not yaml or json...', exc_info=True)
            raise RuntimeError('Unknown config file format. Malformed?')

    core = BDKCore(cfg_dict)
    try:
        core.configure()
        core.start()
    except KeyboardInterrupt:
        # FIXME
        logger.info('Keyboard interrupt, trying graceful shutdown. Do not press interrupt again...')
    except Exception:
        logger.error('Uncaught exception in core\n', exc_info=True)


if __name__ == '__main__':
    set_sig_handler()
    main()
