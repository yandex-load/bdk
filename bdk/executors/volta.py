from bdk.common.interfaces import AbstractExecutor


class VoltaExecutor(AbstractExecutor):
    def __init__(self, config):
        super(VoltaExecutor, self).__init__(config)

    def run(self, job):
        pass
