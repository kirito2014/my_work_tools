# -*- coding:utf-8 -*-
import logging 
import random
import threading
import os


level_list = {"DEBUG":logging.debug,"INFO":logging.info,"WARNING":logging.warning,"ERROR":logging.error,"CRITICAL":logging.critical}
_level = level_list.get(os.environ.get("ETL_LOG_LEVEL"),logging.INFO)

_logmode = "w"
_datefmt =  "%Y-%m-%d %H:%M:%S"
_format =  "%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]: %(message)s"

class Logger:
    instance = None
    mutex = threading.Lock()

    def __init__(self) :
        self.instance = self.get_instance()

    @staticmethod
    def get_instance(para=""):
        if Logger.instance is None:
            Logger.mutex.acquire()
            if Logger.instance is None:
                Logger.instance = get_logger(para)
            else:
                pass
            Logger.mutex.release()
        else:
            pass
        return Logger.instance

def get_logger(path="",level=_level):
    #创建一个logger 
    logger = logging.getLogger(str(random.uniform(1,100)))
    logger.setLevel(level)

    #定义handler的输出格式formatter和日期格式datefmt
    formatter = logging.Formatter(_format, _datefmt)

    #创建一个handler，用于写入日志文件
    #给logger添加一个handler
    ch =  logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    #创建一个handler，用于写入日志文件（覆盖重写）同时输出到控制台
    if path !="":
        fh =  logging.FileHandler(path,mode=_logmode)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

if __name__ == "__main__":
    logger = get_logger(r"D:\vscode\sunline_etl_tools\logslogger_test.log")
    logger.info("info")
    logger.debug("debug")
    logger.warning("warning")
    logger.error("error")
    logger.critical("critical")

    logger_std = get_logger()
    logger_std.debug("debug")
    logger_std.info("info")
    logger_std.warning("warning")
    logger_std.error("error")
    logger_std.critical("critical")

    a = Logger.get_instance(r"D:\vscode\sunline_etl_tools\logslogger_test.log")
    b = Logger.get_instance()

    a.debug('aaaa')
    b.debug('bbbb')