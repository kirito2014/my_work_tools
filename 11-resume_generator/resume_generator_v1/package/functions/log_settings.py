#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
import logging.handlers

# from pathlib import Path

class Logger():
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'critical':logging.CRITICAL
    }
    
    def __init__(self,filename,level = 'info',when = 'D',backupCount = 60,fmt = '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        formatter = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations[level])
        
        if not self.logger.handlers:
            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
        
            th = logging.handlers.TimedRotatingFileHandler(filename = filename,when = when,backupCount = backupCount,encoding = 'utf-8')
            th.setFormatter(formatter)
            
            self.logger.addHandler(sh)
            self.logger.addHandler(th)
 
current_file_path = os.path.dirname(os.path.abspath('__file__'))
# current_file_path = Path(__file__).resolve().parent.parent
# print(current_file_path)
log_file = current_file_path + f'/log/rsm2bank_{time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())}.log'
log = Logger(log_file,level = 'info') 
 
if __name__ == "__main__":
    
    pass