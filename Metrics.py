#!/usr/bin/python -O
"""

   Data sensor to generate cpu metrics

"""

import re
import os
import pwd
import glob
import socket
import time
import commands
from abc import abstractmethod
from threading import Thread, Condition
from qpid.messaging import Connection, Message


#############################################
# constants
DEF_CONTENTTYPE = 'application/x-protobuf'
CLK_TCK = os.sysconf('SC_CLK_TCK')
PAGESIZE = os.sysconf('SC_PAGE_SIZE')

_address_opt = '{create:always, node:{type:topic, durable:False}}'


#############################################
# logs
#

def log_debug(msg):
    if __debug__:
        print msg


def log_info(msg):
    print msg


def log_error(msg):
    print msg


def log_exception(msg):
    print msg


#############################################
# utility functions
#

def get_hostid(buf=[]):
    if len(buf) == 0:
        buf.append(commands.getoutput('hostid').strip())

    return buf[0]


def sequence_generator(start=0):
    seq = [start]
    def sequencer(step=1):
        seq[0] += step
        return seq[0]

    return sequencer

next_sequence_num = sequence_generator()

def set_info_header(header):
    header.info_message_id = next_sequence_num()
    header.info_generation_time_utc = int(time.time() * 1000000)
    header.info_hostid = get_hostid()


def get_all_cpu_infos(cpu_infos={}):
    """
       return a dict of cpu info, key is processor id
       cache the result so we only get the info once
    """
    if len(cpu_infos) == 0:
        cpu = None
        processor_id = None

        for line in file('/proc/cpuinfo'):
            try:
                key, value = re.split(r'\s+:\s+', line.strip(), 1)
                if key == 'processor':
                    if processor_id is not None:
                        cpu_infos[processor_id] = cpu

                    processor_id = int(value)
                    cpu = {}
                else:
                    cpu[key] = value

            except ValueError:  # empty line
                pass

        if processor_id is not None:
            cpu_infos[processor_id] = cpu

    return cpu_infos


def get_all_cpu_stats():
    """
       Return a list containing all the cpu stat data
       the time unit is millisecond
    """
    stats = []
    cpu_infos = get_all_cpu_infos()
    for line in file('/proc/stat'):
        if not line.startswith('cpu'):
            continue
        fields = line.split()
        cpu = fields[0]
        if cpu == 'cpu':
            #the first line is the summary of all cpus, we don't need it
            continue

        processor_id = int(cpu[len('cpu'):])
        stat = {
            'processor'   : processor_id,
            'physical_id' : int(cpu_infos[processor_id].get('physical id', 0)),
            'core_id'     : int(cpu_infos[processor_id].get('core id', 0)),
            'utime' : int(fields[1]) * 1000 / CLK_TCK,
            'nice'  : int(fields[2]) * 1000 / CLK_TCK,
            'stime' : int(fields[3]) * 1000 / CLK_TCK,
            'idle'  : int(fields[4]) * 1000 / CLK_TCK,
            'iowait': int(fields[5]) * 1000 / CLK_TCK,
            'irq':    int(fields[6]) * 1000 / CLK_TCK,
            'softirq':int(fields[7]) * 1000 / CLK_TCK,
            'steal' : 0,
            'guest' : 0
        }

        stat['total'] = stat['utime'] + stat['nice'] + stat['stime'] + \
                stat['idle'] + stat['iowait'] + stat['irq'] + stat['softirq']

        if len(fields) > 8:
            stat['steal'] = int(fields[8]) * 1000 / CLK_TCK

        if len(fields) > 9:
            stat['guest'] = int(fields[9]) * 1000 / CLK_TCK

        stats.append(stat)

    return stats


def get_uptime():
    """ get uptime from /proc/uptime """
    with file('/proc/uptime') as fhuptime:
        uptime = fhuptime.read().split()[0]
        return float(uptime)


def get_proc_stat(pid):
    """
       Return the process statistic data for a specific process id
    """

    try:
        stat_file = '/proc/{0}/stat'.format(pid)

        st = os.stat(stat_file)
        pw = pwd.getpwuid(st.st_uid)

        fields = file(stat_file).readline().split()

        stat = {'pid'   : int(pid),
                'owner' : pw.pw_name,
                'comm'  : fields[1].strip('()'),
                'state' : fields[2],
               }

        # following fields all need to covert to integer
        field_names = [
                'ppid',                      # 3
                'pgrp',                      # 4
                'session',                   # 5
                'tty_nr',                    # 6
                'tpgid',                     # 7
                'flags',                     # 8
                'minflt',                    # 9
                'cminflt',                   # 10
                'majflt',                    # 11
                'cmajflt',                   # 12
                'utime',                     # 13
                'stime',                     # 14
                'cutime',                    # 15
                'cstime',                    # 16
                'priority',                  # 17
                'nice',                      # 18
                'num_threads',               # 19
                'itrealvalue',               # 20
                'starttime',                 # 21
                'vsize',                     # 22
                'rss',                       # 23
                'rsslim',                    # 24
                'startcode',                 # 25
                'endcode',                   # 26
                'startstack',                # 27
                'kstkesp',                   # 28
                'kstkeip',                   # 29
                'signal',                    # 30
                'blocked',                   # 31
                'sigignore',                 # 32
                'sigcatch',                  # 33
                'wchan',                     # 34
                'nswap',                     # 35
                'cnswap',                    # 36
                'exit_signal',               # 37
                'processor',                 # 38
                'rt_priority',               # 39
                'policy',                    # 40
                'delayacct_blkio_ticks',     # 41
                'guest_time',                # 42
                'cguest_time'                # 43
               ]

        for i, field_name in enumerate(field_names):
            try:

                if field_name in ('utime', 'stime',
                                  'cutime', 'cstime',
                                  'guest_time', 'cguest_time',
                                  'starttime'):
                    # convert to seconds
                    stat[field_name] = int(fields[i+3]) * 1000 / CLK_TCK
                else:
                    stat[field_name] = int(fields[i+3])
            except ValueError:
                #convert error, just ignore
                pass
            except IndexError:
                #reach the last field
                break

        stat['runtime_ms'] = int(get_uptime() * 1000 - stat['starttime'])
        stat['memory_bytes'] = stat['rss'] * PAGESIZE

        cmd_file = '/proc/{0}/cmdline'.format(pid)
        stat['cmdline'] = ' '.join(file(cmd_file).readline().split(chr(0)))

        return stat

    except OSError:
        # from os.stat()
        return None
    except IOError:
        # from file(), open file error
        return None
    except KeyError:
        # dict key not found
        return None


def get_all_proc_stats():
    """
        return a dict containing the process stat for all running processes
        key is pid, content is the stat data
    """
    pid_list = [os.path.basename(path) for path in glob.glob('/proc/[0-9]*')
                                       if os.path.exists(path + '/stat')]

    stats = {}
    for pid in pid_list:
        stat = get_proc_stat(pid)
        if not stat is None:
            stats[int(pid)] = stat

    return stats


def get_memory_info():
    """
       get memory information from /proc/meminfo
    """
    mem_info = {}
    with file('/proc/meminfo') as fhmem:
        for line in fhmem:
            fields = line.split(':')
            key = fields[0]
            value = fields[1].strip().split()[0]
            mem_info[key] = int(value)

    return mem_info


#############################################
#classes
#

class QpidConnectionError(Exception):
    pass


class Producer(object):
    """ the producer """
    def __init__(self, broker, exchange):
        conn = Connection(broker, tcp_nodelay=True, transport='tcp')

        try:
            conn.open()
            address = '{0};{1}'.format(exchange, _address_opt)
            session = conn.session()
            self.__sender = session.sender(address)

        except ValueError:
            raise QpidConnectionError('Broker error: {0}'.format(broker))

    def send(self, routing_key, msg_str, msg_type=DEF_CONTENTTYPE):
        """ send a message """
        if routing_key is None:
            return

        msg = Message(subject=routing_key,
                      content_type=msg_type,
                      content=msg_str)

        if __debug__:
            print 'Sending', msg

        self.__sender.send(msg)


class QpidDataSensor(Thread):
    def __init__(self, broker, routing_key=None, interval=60,
                       exchange="amq.topic",
                       skip_first=True):
        super(QpidDataSensor, self).__init__()

        self.daemon = True
        self.__interval = interval
        self.__lastts = time.time() * 1000
        self.__skip_first = skip_first

        self.__cv = Condition()
        self.__exit = False

        self.__producer = Producer(broker, exchange)
        self.__routing_key = routing_key

    @property
    def interval(self):
        return self.__interval

    def _init(self):
        pass

    def run(self):
        """ main loop """
        self._init()

        if self.__skip_first:  # sleep one interval to get delta values
            time.sleep(self.interval)

        ts_start = time.time()
        checkpoints = 60

        while not self.__exit:
            # add a checkpoint
            if checkpoints > 0:
                checkpoints -= 1
            else:
                log_info('Checkpoint')
                checkpoints = 60

            message = self.collect()
            if message:
                self.send(message)

            self.__cv.acquire()
            interval = self.interval - (time.time() - ts_start)

            self.__cv.wait(interval)
            self.__cv.release()
            ts_start += self.interval

    def stop(self):
        """ stop the data sensor, try to exit gracefully """
        self.__cv.acquire()
        self.__exit = True
        self.__cv.notify_all()
        self.__cv.release()

        self.join(self.interval)

    @abstractmethod
    def collect(self):
        """ collect data """
        pass

    def send(self, message, routing_key=None):
        if routing_key is None:
            routing_key = self.__routing_key

        self.__producer.send(routing_key, message)


#############################################
# metricses
#

class CpuMetrics(QpidDataSensor):
    """
       data sensor to get cpu metrics
    """
    def __init__(self, broker_ip, broker_port, routing_key, interval=60):
        super(CpuMetrics, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval)

        self.__cpu_stats = []

    def _init(self):
        self.__cpu_stats = get_all_cpu_stats()

    def collect(self):
        log_debug('Collecting data...')

        from linuxcounters.cpu_metrics_pb2 import cpu_metrics

        prev_stats = self.__cpu_stats
        self.__cpu_stats = get_all_cpu_stats()

        metrics = cpu_metrics()
        set_info_header(metrics.header)

        for cpu_index, cpu_stat in enumerate(self.__cpu_stats):
            metrics.host_name   = socket.getfqdn()
            metrics.processor   = cpu_stat['processor']
            metrics.physical_id = cpu_stat['physical_id']
            metrics.core_id     = cpu_stat['core_id']

            prev_stat = prev_stats[cpu_index]

            delta_total   = cpu_stat['total']   - prev_stat['total']
            delta_utime   = cpu_stat['utime']   - prev_stat['utime']
            delta_nice    = cpu_stat['nice']    - prev_stat['nice']
            delta_stime   = cpu_stat['stime']   - prev_stat['stime']
            delta_idle    = cpu_stat['idle']    - prev_stat['idle']
            delta_iowait  = cpu_stat['iowait']  - prev_stat['iowait']
            delta_irq     = cpu_stat['irq']     - prev_stat['irq']
            delta_softirq = cpu_stat['softirq'] - prev_stat['softirq']
            delta_steal   = cpu_stat['steal']   - prev_stat['steal']
            delta_guest   = cpu_stat['guest']   - prev_stat['guest']

            metrics.avg_total    = 100.0 * (delta_total - delta_idle) \
                                               / delta_total
            metrics.avg_user     = 100.0 * delta_utime   / delta_total
            metrics.avg_nice     = 100.0 * delta_nice    / delta_total
            metrics.avg_system   = 100.0 * delta_stime   / delta_total
            metrics.avg_idle     = 100.0 * delta_idle    / delta_total
            metrics.avg_iowait   = 100.0 * delta_iowait  / delta_total
            metrics.avg_irq      = 100.0 * delta_irq     / delta_total
            metrics.avg_soft_irq = 100.0 * delta_softirq / delta_total
            metrics.avg_steal    = 100.0 * delta_steal   / delta_total
            metrics.avg_guest    = 100.0 * delta_guest   / delta_total

            log_debug('Sending {0}'.format(metrics))
            self.send(metrics.SerializeToString())


class TopNBase(QpidDataSensor):
    def __init__(self, broker, routing_key, interval=60):
        super(TopNBase, self).__init__(broker, routing_key, interval)

    @property
    def number(self):
        return 20


class TopNCpu(TopNBase):
    """ Data sensor to produce process list for topn cpu usage """
    def __init__(self, broker_ip, broker_port, routing_key, interval=60):
        super(TopNCpu, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval)

        self.__cpu_stats = get_all_cpu_stats()
        self.__proc_stats = get_all_proc_stats()

    def collect(self):
        """ collect process statistics for topn cpu"""
        log_debug('Collecting data for cpu metric')


        p_cpus = self.__cpu_stats
        p_procs = self.__proc_stats
        self.__cpu_stats = get_all_cpu_stats()
        self.__proc_stats = get_all_proc_stats()

        for pid in self.__proc_stats.keys():
            # calculate cpu usage (percentage) for each process
            proc = self.__proc_stats[pid]
            cpu_index = proc['processor']
            if pid in p_procs:
                delta_proc = proc['runtime_ms'] - p_procs[pid]['runtime_ms']
            else:
                delta_proc = proc['runtime_ms']

            delta_cpu = self.__cpu_stats[cpu_index]['total'] - \
                            p_cpus[cpu_index]['total']

            proc['cpu_pct'] = 100 * delta_proc / delta_cpu

        proc_list = sorted(self.__proc_stats.values(),
                           key=lambda proc: proc['cpu_pct'],
                           reverse=True)

        from linuxcounters.topn_cpu_pb2 import topn_cpu
        process = topn_cpu()
        set_info_header(process.header)
        sort_order = 1

        for proc_stat in proc_list[: self.number]:
            process.sort_order = sort_order
            for key, value in proc_stat.items():
                if hasattr(process, key):
                    setattr(process, key, value)

            sort_order += 1

            log_debug('Sending {0}'.format(process))
            self.send(process.SerializeToString())


class TopNMemory(TopNBase):
    """ Data sensor to produce process list for topn memory usage """
    def __init__(self, broker_ip, broker_port, routing_key, interval=60):
        super(TopNMemory, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval)

    def collect(self):
        """ collect process statistics for topn memory"""
        log_debug('Collecting data for memory metric')

        proc_list = sorted(get_all_proc_stats().values(),
                           key=lambda proc: proc['memory_bytes'],
                           reverse=True)

        from linuxcounters.topn_memory_pb2 import topn_memory
        process = topn_memory()
        set_info_header(process.header)

        sort_order = 1

        for proc_stat in proc_list[: self.number]:
            process.sort_order = sort_order
            for key, value in proc_stat.items():
                if hasattr(process, key):
                    setattr(process, key, value)

            sort_order += 1

            log_debug('Sending {0}'.format(process))
            self.send(process.SerializeToString())


class TopNRuntime(TopNBase):
    """ Data sensor to produce process list for topn runtime """
    def __init__(self, broker_ip, broker_port, routing_key, interval=60):
        super(TopNRuntime, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval)

    def collect(self):
        """ collect process statistics for topn runtime"""
        log_debug('Collecting data for runtime metric')

        proc_list = sorted(get_all_proc_stats().values(),
                           key=lambda proc: proc['runtime_ms'],
                           reverse=True)

        from linuxcounters.topn_runtime_pb2 import topn_runtime
        process = topn_runtime()
        set_info_header(process.header)
        sort_order = 1

        for proc_stat in proc_list[: self.number]:

            process.sort_order = sort_order
            for key, value in proc_stat.items():
                if hasattr(process, key):
                    setattr(process, key, value)

            sort_order += 1

            log_debug('Sending {0}'.format(process))
            self.send(process.SerializeToString())


class TopNSwap(TopNBase):
    """ Data sensor to produce process list for topn swap usage """
    def __init__(self, broker_ip, broker_port, routing_key, interval=60):
        super(TopNSwap, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval)

    def collect(self):
        """ collect process statistics for topn swap"""
        log_debug('Collecting data for swap metric')

        proc_stats = get_all_proc_stats()
        for pid in proc_stats.keys():
            # calculate swap usage for each process
            proc = proc_stats[pid]

            try:
                for line in file('/proc/{0}/status'.format(pid)):
                    if line.startswith('VmSwap:'):
                        swap_k = line.strip().split()[1]
                        proc['swap_bytes'] = int(swap_k) * 1024
                        break
                else:
                    proc['swap_bytes'] = 0
            except IOError:
                proc['swap_bytes'] = 0

        proc_list = sorted(proc_stats.values(),
                           key=lambda proc: proc['swap_bytes'],
                           reverse=True) #from large to small

        from linuxcounters.topn_swap_pb2 import topn_swap
        process = topn_swap()
        set_info_header(process.header)
        sort_order = 1

        for proc_stat in proc_list[: self.number]:
            process.sort_order = sort_order
            for key, value in proc_stat.items():
                if hasattr(process, key):
                    setattr(process, key, value)

            sort_order += 1

            log_debug('Sending {0}'.format(process))
            self.send(process.SerializeToString())


class MemoryMetrics(QpidDataSensor):
    """
       data sensor to get cpu metrics
    """
    def __init__(self, broker_ip, broker_port, routing_key, interval):
        super(MemoryMetrics, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval)

    def collect(self):
        """ collect network statistics """
        log_debug('Collecting data...')

        from linuxcounters.memory_metrics_pb2 import memory_metrics

        metrics = memory_metrics()
        set_info_header(metrics.header)

        meminfo = get_memory_info()

        metrics.total       = meminfo['MemTotal'] * 1024
        metrics.free        = meminfo['MemFree'] * 1024
        metrics.buffers     = meminfo['Buffers'] * 1024
        metrics.cached      = meminfo['Cached'] * 1024
        metrics.swap_cached = meminfo['SwapCached'] * 1024
        metrics.active      = meminfo['Active'] * 1024
        metrics.inactive    = meminfo['Inactive'] * 1024
        metrics.swap_total  = meminfo['SwapTotal'] * 1024
        metrics.swap_free   = meminfo['SwapFree'] * 1024

        log_debug('Sending {0}'.format(metrics))
        self.send(metrics.SerializeToString())

