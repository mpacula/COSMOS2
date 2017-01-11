# -*- coding: utf-8 -*-

"""\
(c) 2015-2016 MGH Center for Integrated Diagnostics

"""

from __future__ import unicode_literals
from __future__ import print_function

import os
import subprocess as sp

from .DRM_Base import DRM


class Ec2WorkerTask(object):
    def __init__(self, host, pid):
        self.host = host
        self.pid = pid

    def __str__(self):
        return str('host: {}, pid: {}'.format(self.host, self.pid))


def check_ssh(host, cmd):
    return sp.check_output('ssh -o "StrictHostKeyChecking no" {host} \'{cmd_str}\''.format(host=host, cmd_str=cmd),
                           env=os.environ,
                           preexec_fn=preexec_function(),
                           shell=True)


class DRM_EC2(DRM):
    """Proof-of-concept DRM for EC2"""
    name = 'ec2'

    def __init__(self, *args, **kwargs):
        super(DRM_EC2, self).__init__(*args, **kwargs)
        self.workers = ['172.31.20.154']

    def get_worker(self, task):
        return self.workers[hash(task) % len(self.workers)]  # pick one at random

    def submit_job(self, task):
        worker_name = self.get_worker(task)

        # This will start the command under nohup, then echo $! will return a PID on the remote machine
        # which we store as the ID for the job
        out = check_ssh(worker_name, 'nohup {cmd_str} > {stdout} 2> {stderr} < /dev/null & echo $!\''.format(
            cmd_str=self.jobmanager.get_command_str(task),
            stdout=task.output_stdout_path,
            stderr=task.output_stderr_path))

        remote_pid = int(out)
        return Ec2WorkerTask(worker_name, remote_pid)

    def drm_statuses(self, tasks):
        return {}


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()
