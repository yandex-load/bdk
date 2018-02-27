import logging
from bdk.common.interfaces import AbstractExecutor

logger = logging.getLogger(__name__)


class YandexTankExecutor(AbstractExecutor):
    def __init__(self, config):
        super(YandexTankExecutor, self).__init__(config)
        pass

    def run(self, job):
        pass
