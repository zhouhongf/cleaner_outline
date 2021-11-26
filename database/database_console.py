from importlib import import_module
from config import Logger
import time


def database_console():
    start = time.time()

    mongodb_module = import_module("database.backends.mongo_database")
    print('================== 执行outline_to_wealth()方法')
    mongodb_module.outline_to_wealth()

    logger = Logger(level='warning').logger
    logger.info("Time costs: {0}".format(time.time() - start))


if __name__ == '__main__':
    database_console()
