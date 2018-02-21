
#############################################
# logs
#

from __future__ import print_function

def log_debug(msg):
    if __debug__:
        print('Debug:', msg)


def log_info(msg):
    print('Info:', msg)


def log_error(msg):
    print('Error:', msg)


def log_exception(msg):
    print('Expection:', msg)


