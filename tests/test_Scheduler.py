# Test suite for Scheduler module
import shutil

from Schedulers import conf, Scheduler
from Jobs import Job
from nose.tools import eq_, ok_, raises
import os
from Services import ServiceStore, Service, Validator


def setup_module():
    """
    Some configuration options to adapt environment for testing
    :return:
    """
    test_assets = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
    # adapt config for testing purposes
    conf.gate_path_jobs = os.path.join(test_assets, 'payloads')
    conf.service_path_data = os.path.join(test_assets, 'services','Data')
    # add testing services to services list
    for _service in ['test', 'default', 'basic']:
        with open(os.path.join(test_assets, 'services', _service+'.json')) as _f:
            ServiceStore[_service] = Service(_service, conf.json_load(_f))


class TestGenerateScripts(object):

    @classmethod
    def setup_class(cls):
        import tempfile
        cls.scheduler = Scheduler()
        work_dir = os.path.join(tempfile.gettempdir(), "services_data")
        #clean workdir before tests
        if os.path.isdir(work_dir):
            shutil.rmtree(work_dir)
        os.mkdir(work_dir)
        cls.scheduler.work_path = work_dir


    def test_proper_simple_input(self):
        """
        Scheduler.generate_scripts for proper simple input
        :return:
        """
        job = Job('basic_valid.json')
        # here i could create tmp object to spoofing Job for testing purposes,
        # but for now I'm too lazy and I have trust that 'validate' function is flawless
        # TODO change that
        Validator.validate(job)
        ######
        ok_(self.scheduler.generate_scripts(job), "Generating script")
        with open(os.path.join(self.scheduler.work_path, job.id(), 'pbs.sh')) as _f:
            out_lines = _f.readlines()
        ok_(len(out_lines) > 0, 'Output file is empty')
        valid_lines = ["#!/bin/sh",
                        "one 30",
                        "100 two",
                        "nanana -20.1 nanana",
                        "99.2",
                        "[1.2, 2.1, 77.4]"]
        for i,line in enumerate(zip(valid_lines, out_lines)):
            # 0 - valid, 1 - output
            eq_(line[0], line[1].rstrip())