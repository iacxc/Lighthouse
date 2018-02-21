
from abc import abstractmethod
import importlib
import os
import operator
import sys
import threading
import time

from Funs import sequence_generator, get_hostid, get_all_proc_stats, \
                 get_all_cpu_stats
from Logs import log_info, log_debug


__all__ = ('LoadAvgMetrics',
          )

############################################
# utility functions and classes
#
sequence_num = sequence_generator()

def set_info_header(header):
    header.info_message_id = next(sequence_num)
    header.info_generation_time_utc = int(time.time() * 1000000)
    header.info_hostid = get_hostid()


def load_module(module):
    import zipimport
    for _ in range(3):
        try:
            pkg = importlib.import_module(module)
            return pkg
        except zipimport.ZipImportError:
            log_debug('zlib failure for {0}, try again'.format(module))
            time.sleep(0.1)
            pass


class PeriodicTimer(object):
    def __init__(self, interval):
        self._interval = interval
        self._flag = 0
        self._cv = threading.Condition()

    def start(self):
        t = threading.Thread(target=self.run)
        t.daemon = True
        t.start()

    def run(self):
        while True:
            time.sleep(self._interval)
            with self._cv:
                self._flag ^= 1
                self._cv.notify_all()

    def wait_for_tick(self):
        with self._cv:
            last_flag = self._flag
            while last_flag == self._flag:
                self._cv.wait()


#############################################
# metricses
class MetricsBase(threading.Thread):
    def __init__(self, 
                 routing_key=None, interval=60, handler=None, skip_first=True):
        super().__init__()

        self._routing_key = routing_key
        self._interval = interval
        self._handler = handler
        self._skip_first = skip_first

        self.daemon = True
        self._timer = PeriodicTimer(self.interval)
        self._exit = False

    @property
    def interval(self):
        return self._interval

    @property
    def routing_key(self):
        return self._routing_key

    def _init(self):
        pass

    def run(self):
        """ main loop """
        self._init()
        self._timer.start()

        if self._skip_first:  # sleep one interval to get delta values
            self._timer.wait_for_tick()

        for message in self.collect():
            if self._exit:
                return
            if message:
                self.send(message)
            self._timer.wait_for_tick()

    def stop(self):
        """ stop the data sensor, try to exit gracefully """
        self._exit = True
        self.join(self.interval)
        log_debug('Finished')

    @abstractmethod
    def collect(self):
        """ collect data """
        pass

    def send(self, message, routing_key=None):
        if routing_key is None:
            routing_key = self.routing_key

        if self._handler:
            self._handler(routing_key, message)


class LoadAvgMetrics(MetricsBase):
    """
       data sensor to get loadavg
    """
    def __init__(self, routing_key=None, interval=60, handler=None):
        super().__init__(routing_key, interval, handler, False)

    def collect(self):
        """ collect loadavg statistics """
        mod = load_module('linuxcounters.loadavg_metrics_pb2')
        metrics = mod.loadavg_metrics()
        while True:
            set_info_header(metrics.header)

            log_debug('Collecting data for loadavg')
            metrics.one_min_avg, metrics.five_min_avg, metrics.fifteen_min_avg =\
                os.getloadavg()

            log_debug('Sending {0}'.format(metrics))
            yield metrics.SerializeToString()


class TopNBase(MetricsBase):
    msg_mod_name = None
    mod_class_name = None

    def __init__(self, 
                 routing_key=None, interval=60, handler=None, skip_first=True):
        super().__init__(routing_key, interval, handler, skip_first)

    @property
    def number(self):
        return 20

    @property
    @abstractmethod
    def sort_key(self):
        pass

    def create_process(self):
        mod = load_module(self.msg_mod_name)
        klass = getattr(mod, self.msg_class_name)
        return klass()

    @property
    def proc_list(self):
        return get_all_proc_stats().values()

    def collect(self):
        """ collect process statistics for topn memory"""
        process = self.create_process()
        while True:
            set_info_header(process.header)

            log_debug('Collecting data for topn memory metric')
            proc_list = sorted(self.proc_list, key=self.sort_key, reverse=True)
            sort_order = sequence_generator(1)
            for proc_stat in proc_list[:self.number]:
                process.sort_order = next(sort_order)
                for key, value in proc_stat.items():
                    if hasattr(process, key):
                        setattr(process, key, value)

                log_debug('Sending {0}'.format(process))
                self.send(process.SerializeToString())

            yield


class TopNMemory(TopNBase):
    """ Data sensor to produce process list for topn memory usage """
    msg_mod_name = 'linuxcounters.topn_memory_pb2'
    mod_class_name = 'topn_memory'

    def __init__(self, routing_key=None, interval=60, handler=None):
        super().__init__(routing_key, interval, handler, False)

    @property
    def sort_key(self):
        return operator.itemgetter('memory_bytes')


class TopNRuntime(TopNBase):
    """ Data sensor to produce process list for topn runtime """
    msg_mod_name = 'linuxcounters.topn_runtime_pb2'
    mod_class_name = 'topn_runtime'

    def __init__(self, routing_key=None, interval=60, handler=None):
        super().__init__(routing_key, interval, handler, False)

    @property
    def sort_key(self):
        return operator.itemgetter('runtime_ms')


class TopNCpu(TopNBase):
    """ Data sensor to produce process list for topn cpu usage """
    msg_mod_name = 'linuxcounters.topn_cpu_pb2'
    msg_class_name = 'topn_cpu'

    def __init__(self, routing_key=None, interval=60, handler=None):
        super().__init__(routing_key, interval, handler, True)

        self.__cpu_stats = get_all_cpu_stats()
        self.__proc_stats = get_all_proc_stats()

    @property
    def sort_key(self):
        return operator.itemgetter('cpu_pct')

    @property
    def proc_list(self):
        p_cpus, p_procs = self.__cpu_stats, self.__proc_stats # previous one

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

            proc['cpu_pct'] = int(100 * delta_proc / delta_cpu)

        return self.__proc_stats.values()


class TopNSwap(TopNBase):
    """ Data sensor to produce process list for topn swap usage """
    msg_mod_name = 'linuxcounters.topn_swap_pb2'
    msg_class_name = 'topn_swap'

    def __init__(self, routing_key=None, interval=60, handler=None):
        super().__init__(routing_key, interval, handler, False)

    @property
    def sort_key(self):
        return operator.itemgetter('swap_bytes')

    @property
    def proc_list(self):
        proc_stats = get_all_proc_stats()
        for pid in proc_stats.keys():
            # calculate swap usage for each process
            proc = proc_stats[pid]

            try:
                with open('/proc/{0}/status'.format(pid)) as f:
                    for line in f:
                        if line.startswith('VmSwap:'):
                            swap_k = line.strip().split()[1]
                            proc['swap_bytes'] = int(swap_k) * 1024
                            break
                    else:
                        proc['swap_bytes'] = 0
            except IOError:
                proc['swap_bytes'] = 0

        return proc_stats.values()


if __name__ == '__main__':
    def set_proto_src(path):
        """ set the path of publications for google protocol buffer """
        if sys.path.count(path) == 0:
            sys.path.append(path)

    def print_handler(*args):
        #print(time.time(), args)
        print(time.time(), args[0])

    set_proto_src('/c/caiche/Github/Lighthouse/lh_publications.zip')
    m = TopNSwap(routing_key='linuxcounters.topnswap', 
                   interval=3, handler=print_handler)
#    m2 = LoadAvgMetrics(routing_key='linuxcounters.loadavg', 
#                   interval=3, handler=print_handler)
#    m2.start()
    m.start()

    time.sleep(10)
    m.stop()
#    m2.stop()

    time.sleep(3)