#!/usr/bin/python3 -O

import sys
import time

from Logs import log_info, log_debug
from Metrics3 import TopNCpu, TopNMemory, TopNRuntime, TopNSwap, \
                      LoadAvgMetrics


LINUXCOUNTERS = 'linuxcounters'


def set_proto_src(path):
    """ set the path of publications for google protocol buffer """
    if sys.path.count(path) == 0:
        sys.path.append(path)


def print_handler(*args):
    #print(time.time(), args)
    print(time.time(), args[0])


if __name__ == '__main__':
    """ main function """
    import argparse

    sensor_dict = {'topn_cpu': TopNCpu,
                   'topn_memory': TopNMemory,
                   'topn_runtime': TopNRuntime,
                   'topn_swap': TopNSwap,
                   'loadavg': LoadAvgMetrics,
                   }

    parser = argparse.ArgumentParser(description='Lighthouse')
    parser.add_argument('datasensor', 
            action='store', choices=sensor_dict.keys(),
            help='datasensor to run')
    parser.add_argument('-f', '--interval', dest = 'interval', 
            type=int, default=60, help='interval')
    parser.add_argument('--proto-src', dest ='protoSrc', 
            default = './lh_publications.zip',
            help='directory or file for proto definitions')

    args = parser.parse_args()

    sensor_class = sensor_dict.get(args.datasensor)

    set_proto_src(args.protoSrc)
    try:
        sensor = sensor_class('.'.join([LINUXCOUNTERS, args.datasensor]),
                              interval=args.interval,
                              handler=print_handler)

        log_info('Starting...')
#        sensor.run()
        sensor.start()
        time.sleep(10)
        sensor.stop()


    except KeyboardInterrupt:
        log_debug('Control-C pressed, exit ...')
