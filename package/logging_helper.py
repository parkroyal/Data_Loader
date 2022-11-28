import logging
from datetime import date
import os
import traceback

log_folder = "logs"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# set logger name: if import 
logger = logging.getLogger(__name__)
# setting level
logger.setLevel(logging.DEBUG)
# setting log format
formatter = logging.Formatter('%(filename)s - %(name)s - %(funcName)s - %(asctime)s - %(levelname)s :%(message)s')
# write data to x.log
file_name = os.path.join(log_folder, f'{date.today()}.log')
file_handler = logging.FileHandler(file_name)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)


stream_formatter = logging.Formatter('%(levelname)s - %(funcName)s: %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(stream_formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

traceback_printed = True

def log_exception(func):
    """
    Decorator provides simple exception handler: logs exception and throws it up
    :param func:
    :return: None or exception
    """
    def inner(*args, **kwargs):
        global traceback_printed
        try:
            res = func(*args, **kwargs)
            #log.debug('end {}'.format(func.__name__))
        except Exception as e:
            if traceback_printed:
                logger.error('{} {}'.format(func.__name__, e))
            else:
                logger.error('{} {}. \ntraceback: {}'.format(func.__name__, e, traceback.format_exc()))
                traceback_printed = True
            raise e
        return res

    return inner