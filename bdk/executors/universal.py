# -*- coding: utf-8 -*-
import logging
import subprocess
import shlex

from bdk.common.interfaces import AbstractExecutor

logger = logging.getLogger(__name__)


class UniversalExecutor(AbstractExecutor):
    def __init__(self, config):
        super(UniversalExecutor, self).__init__(config)
        self.executable = config.get_option('executable', 'cmd')
        self.params = config.get_option('executable', 'params')
        self.shell = config.get_option('executable', 'shell')

    def run(self, job_conf_file, stdout_callback):
        prepared_params = []
        for param in self.params:
            prepared_params = prepared_params + self.__expand_param(
                param, {
                    'job_config': job_conf_file
                }
            )
        logger.debug(u'Prepared params: %s', prepared_params)
        self.cmdline = u"{executable} {params}".format(
            executable=self.executable,
            params=" ".join(prepared_params)
        )
        logger.info('Starting %s', self.cmdline)
        splitted_encoded_args = shlex.split(self.cmdline.encode('utf8'))
        if self.shell:
            popen = subprocess.Popen(self.cmdline, shell=True, stdout=subprocess.PIPE)
        else:
            popen = subprocess.Popen(splitted_encoded_args, stdout=subprocess.PIPE)
        logger.info('Started %s', self.cmdline)
        for stdout_line in iter(popen.stdout.readline, ""):
            logger.info(stdout_line)
            stdout_callback(stdout_line)
        popen.stdout.close()
        return_code = popen.wait()
        return return_code

    @staticmethod
    def __expand_param(param, variable_params):
        parsed_params = []
        if isinstance(param, basestring):
            if '{job_config}' in param:
                param = param.format(job_config=variable_params.get('job_config'))
            parsed_params.append(param)
        elif isinstance(param, (list, tuple)):
            for key in param:
                if '{job_config}'in key:
                    key = key.format(job_config=variable_params.get('job_config'))
                parsed_params.append(key)
        elif isinstance(param, dict):
            for key, value in param.items():
                if '{job_config}' in key:
                    key = key.format(job_config=variable_params.get('job_config'))
                if '{job_config}' in value:
                    value = value.format(job_config=variable_params.get('job_config'))
                parsed_params.append(
                    "{key} {value}".format(key=key, value=value)
                )
        return parsed_params
