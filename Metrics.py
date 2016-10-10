"""

   Data sensor to generate cpu metrics

"""

import socket
import os
import time
from abc import abstractmethod
from threading import Thread, Condition
from qpid.messaging import Connection, Message

from Logs import log_debug, log_info
from Funs import get_hostid, get_all_cpu_stats, get_all_proc_stats, \
                 get_memory_info, is_dev_fstype, get_mtab


#############################################
# constants
DEF_CONTENTTYPE = 'application/x-protobuf'

_address_opt = '{create:always, node:{type:topic, durable:False}}'


############################################
# utility functions
#

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


#############################################
# classes
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
                       exchange="amq.topic", skip_first=True):
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
    def __init__(self, broker, routing_key, interval=60, skip_first=True):
        super(TopNBase, self).__init__(broker, routing_key, interval, skip_first)

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
            routing_key, interval, skip_first=False)

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
            routing_key, interval, skip_first=False)

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
            routing_key, interval, skip_first=False)

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
       data sensor to get memory metrics
    """
    def __init__(self, broker_ip, broker_port, routing_key, interval):
        super(MemoryMetrics, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval, skip_first=False)

    def collect(self):
        """ collect memory statistics """
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


class LoadAvg(QpidDataSensor):
    """
       data sensor to get loadavg
    """
    def __init__(self, broker_ip, broker_port, routing_key, interval):
        super(LoadAvg, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval, skip_first=False)

    def collect(self):
        """ collect loadavg statistics """
        log_debug('Collecting data...')

        from linuxcounters.loadavg_metrics_pb2 import loadavg_metrics

        metrics = loadavg_metrics()
        set_info_header(metrics.header)

        loadavg = os.getloadavg()

        metrics.one_min_avg     = loadavg[0]
        metrics.five_min_avg    = loadavg[1]
        metrics.fifteen_min_avg = loadavg[2]

        log_debug('Sending {0}'.format(metrics))
        self.send(metrics.SerializeToString())


class FileSystemMetrics(QpidDataSensor):
    """
       data sensor to get filesystem metrics
    """
    def __init__(self, broker_ip, broker_port, routing_key, interval):
        super(FileSystemMetrics, self).__init__(
            '{0}:{1}'.format(broker_ip, broker_port),
            routing_key, interval, skip_first=False)

    def collect(self):
        """ collect loadavg statistics """
        log_debug('Collecting data...')

        from linuxcounters.filesystem_metrics_pb2 import filesystem_metrics

        metrics = filesystem_metrics()
        set_info_header(metrics.header)

        for mnt_entry in get_mtab():
            if is_dev_fstype(mnt_entry['type']):
                metrics.mnt_fsname = mnt_entry['fsname'];
                metrics.mnt_dir    = mnt_entry['dir'];
                metrics.mnt_type   = mnt_entry['type'];
                metrics.mnt_opts   = mnt_entry['opts'];
                metrics.mnt_freq   = mnt_entry['freq'];
                metrics.mnt_passno = mnt_entry['passno'];

                vfs = os.statvfs(mnt_entry['dir'])
                metrics.f_bsize    = vfs.f_bsize;
                metrics.f_frsize   = vfs.f_frsize;
                metrics.f_blocks   = vfs.f_blocks;
                metrics.f_bfree    = vfs.f_bfree;
                metrics.f_bavail   = vfs.f_bavail;
                metrics.f_files    = vfs.f_files;
                metrics.f_ffree    = vfs.f_ffree;
                metrics.f_favail   = vfs.f_favail;
                metrics.f_fsid     = 0;
                metrics.f_flag     = vfs.f_flag;
                metrics.f_namemax  = vfs.f_namemax;

                log_debug('Sending {0}'.format(metrics))
                self.send(metrics.SerializeToString())

