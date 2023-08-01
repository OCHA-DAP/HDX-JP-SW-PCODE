import logging
import time

logger = logging.getLogger(__name__)


def do_nothing_for_ever():
    while True:
        logger.info('Worker is relaxing as it is not enabled')
        time.sleep(300)
