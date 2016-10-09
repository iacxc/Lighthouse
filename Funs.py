"""

   Data sensor to generate cpu metrics

"""

import re
import os
import pwd
import glob
import commands

#############################################
# constants
CLK_TCK = os.sysconf('SC_CLK_TCK')
PAGESIZE = os.sysconf('SC_PAGE_SIZE')


def get_hostid(buf=[]):
    if len(buf) == 0:
        buf.append(commands.getoutput('hostid').strip())

    return buf[0]


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

