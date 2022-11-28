from configlayer.config_helper import get_config
import os
print(os.getcwd())

# import sys 
# print(sys.path)

config, err = get_config(config_folder=r'test\eod_report\configs', report_name='eod_report')

attrs = vars(config)
print(r', \br'.join("%s: %s" % item for item in attrs.items()))
# print

# print(type(test))