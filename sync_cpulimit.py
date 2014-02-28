import os
import time
import subprocess

cpulimit_procs = {}
worker_cpu_limit = int(os.environ.get("TAPIRIIK_WORKER_CPU_LIMIT", 4))

while True:
    active_pids = [pid for pid in os.listdir('/proc') if pid.isdigit()] # Sorry, operating systems without procfs
    for pid in active_pids:
        try:
            proc_cmd = open("/proc/%s/cmdline" % pid, "r").read()
        except IOError:
            continue
        else:
            if "sync_worker.py" in proc_cmd:
                if pid not in cpulimit_procs or cpulimit_procs[pid].poll():
                    cpulimit_procs[pid] = subprocess.Popen(["cpulimit", "-l", str(worker_cpu_limit), "-p", pid])

    for k in list(cpulimit_procs.keys()):
        if cpulimit_procs[k].poll():
            cpulimit_procs[k].wait()
            del cpulimit_procs[k]

    time.sleep(1)
