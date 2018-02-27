class AbstractExecutor(object):
    """ My life for Aiur!  """
    def __init__(self, config):
        self.config = config
        self.cmdline = None

    def run(self, job):
        raise RuntimeError('Abstract method should be overriden')