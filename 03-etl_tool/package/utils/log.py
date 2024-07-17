#!/user/bin/env python3.7
# -*- coding:utf-8 -*-

"""doc of a test module
docs...

"""

import logging
import random
import threading
import os

# ---------------------------------------------------------------------------
#   logger module data
# ---------------------------------------------------------------------------
level_list = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARN": logging.WARN, "ERROR": logging.ERROR}
_level = level_list.get(os.environ.get("ETL_LOG_LEVEL"), logging.INFO)

_logmode = "w"
_datefmt = "%Y-%m-%d %H:%M:%S"
_format = "%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]: %(message)s"
# _format = "%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s]: %(message)s"


class Logger:
    instance = None
    mutex = threading.Lock()

    def __init__(self):
        self.instance = self.get_instance()

    @staticmethod
    def get_instance(para=""):
        if Logger.instance is None:
            Logger.mutex.acquire()
            if Logger.instance is None:
                Logger.instance = get_logger(para)
                # print("初始化实例: %s" % para)
            else:
                # print("实例已初始化")
                pass
            Logger.mutex.release()
        else:
            # print("实例已初始化")
            pass

        return Logger.instance


def get_logger(path="", level=_level):
    # 创建一个logger
    logger = logging.getLogger(str(random.uniform(1, 100)))
    logger.setLevel(level)

    # 定义handler的输出格式formatter和日期格式datefmt
    formatter = logging.Formatter(_format, _datefmt)

    # 1.创建一个handler，用于写入控制台
    # 给logger添加handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 2.创建一个handler，用于写入日志文件（覆盖重写）同时输出到控制台
    if path != "":
        fh = logging.FileHandler(path, mode=_logmode)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

if __name__ == "__main__":
    logger = get_logger("/etlscript/logs/20170101/logger_test.log")
    logger.debug("logger debug message")
    logger.info("logger info message")
    logger.warning("logger warning message")
    logger.error("logger error message")
    logger.critical("logger critical message")

    logger_std = get_logger()
    logger_std.debug("logger debug message")
    logger_std.info("logger info message")
    logger_std.warning("logger warning message")
    logger_std.error("logger error message")
    logger_std.critical("logger critical message")

    a = Logger.get_instance("/tmp/log.txt")
    b = Logger.get_instance()

    a.debug("asdf")
    b.info("asdfa")