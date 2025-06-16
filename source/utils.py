# utils.py

import logging
import time

def setup_logging(log_file_name=None):
    """
    设置日志记录器，使其同时输出到控制台和文件。
    """
    if log_file_name is None:
        log_file_name = f"quant_project_{time.strftime('%Y%m%d')}.log"

    logger = logging.getLogger('MyQuantProject')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if not logger.handlers:
        # FileHandler
        file_handler = logging.FileHandler(log_file_name, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # StreamHandler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

# 您未来还可以把其他通用的函数都加到这个文件里
# def another_useful_function():
#     ...