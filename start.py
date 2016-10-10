#!/usr/bin/python -O


import sys
from Logs import log_info, log_debug
from Metrics import CpuMetrics, TopNCpu, TopNMemory, TopNRuntime, TopNSwap, \
                    MemoryMetrics, LoadAvg, FileSystemMetrics

LINUXCOUNTERS = 'linuxcounters'


def set_proto_src(path):
    """ set the path of publications for google protocol buffer """
    if sys.path.count(path) == 0:
        sys.path.append(path)


if __name__ == '__main__':
    """ main function """
    import qpid.messaging.exceptions
    from optparse import OptionParser

    sensor_dict = {'cpu_metrics': CpuMetrics,
                   'memory_metrics': MemoryMetrics,
                   'topn_cpu': TopNCpu,
                   'topn_memory': TopNMemory,
                   'topn_runtime': TopNRuntime,
                   'topn_swap': TopNSwap,
                   'loadavg_metrics': LoadAvg,
                   'filesystem_metrics': FileSystemMetrics,
                   }

    parser = OptionParser()
    parser.add_option('-i', '--ip', dest='ip', default='127.0.0.1',
          help='ip address of the broker, [default: %default]')
    parser.add_option('-p', '--port', dest='port', type=int, default=5672,
          help='port of the broker, [default: %default]')
    parser.add_option('-d', '--datasensor', dest = 'datasensor', 
          help='datasensor to run, must be in [%s]' % ','.join(sensor_dict.keys()))
    parser.add_option('-f', '--interval', dest = 'interval', 
          type=int, default=60,
          help='interval, [default: %default]')
    parser.add_option('--proto-src', dest ='protoSrc', 
          default = './lh_publications.zip',
          help='directory or file for proto definitions, [default: %default]')

    (opts, _args) = parser.parse_args()

    if opts.datasensor is None:
        parser.print_help()
        sys.exit(1)

    sensor_klass = sensor_dict.get(opts.datasensor)

    if sensor_klass is None:
        parser.print_help()
        sys.exit(1)

    set_proto_src(opts.protoSrc)
    try:
        sensor = sensor_klass(opts.ip, opts.port,
                              '.'.join([LINUXCOUNTERS, opts.datasensor]),
                              opts.interval)

        log_info('Starting...')
        sensor.run()

    except qpid.messaging.exceptions.ConnectError:
        log_error('Connection refused {0}:{1}'.format(opts.ip,
                                                      opts.port))

    except KeyboardInterrupt:
        log_debug('Control-C pressed, exit ...')




