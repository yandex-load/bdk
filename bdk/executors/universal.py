import logging
import subprocess
import sys

from bdk.common.interfaces import AbstractExecutor

logger = logging.getLogger(__name__)


class UniversalExecutor(AbstractExecutor):
    def __init__(self, config):
        super(UniversalExecutor, self).__init__(config)
        self.executable = config.get_option('executable', 'cmd')
        self.params = config.get_option('executable', 'params')

    def run(self, jobconffile):
        prepared_params = []
        for param in self.params:
            prepared_params = prepared_params + self.__expand_param(param, jobconffile)
        logger.debug('Prepared params: %s', prepared_params)
        self.cmdline = "{executable} {params}".format(
            executable=self.executable,
            params=" ".join(prepared_params)
        )
        logger.info('Starting %s', self.cmdline)
        subprocess.call([
            part.decode(sys.getfilesystemencoding()) for part in self.cmdline.split()
        ])

    @staticmethod
    def __expand_param(param, jobconffile):
        if isinstance(param, basestring):
            if param == '$conf':
                param = jobconffile
            return [param]

        if isinstance(param, dict):
            res = []
            for key, value in param.items():
                if key == '$conf':
                    key = jobconffile
                if value == '$conf':
                    value = jobconffile
                res.append("{key} {value}".format(key=key, value=value))
            return res
