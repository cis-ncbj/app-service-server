# simple script that tests how service files are generated

from Schedulers import conf, Scheduler
from Jobs import Job
from nose.tools import eq_, ok_, raises
import os
from Services import ServiceStore, Service, Validator


def prepare_environment():
    _services_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Services')
    # adapt config for testing purposes
    conf.service_path_data = os.path.join(_services_dir, 'Data')
    # add testing services to services list
    for _service in ['AppFlexpart', 'default']:
        with open(os.path.join(_services_dir, _service)) as _f:
            ServiceStore[_service] = Service(_service, conf.json_load(_f))

if __name__ == "__main__":
    prepare_environment()
    test_payload = os.path.join("C:\Users\gomulskik\Desktop\out_test\AppFlexpart_test.json")
    filename = os.path.basename(test_payload)
    test_dir = os.path.dirname(os.path.realpath(test_payload))
    conf.gate_path_jobs = test_dir

    print test_dir
    scheduler = Scheduler()
    work_dir = os.path.join(test_dir, 'work')
    scheduler.work_path = work_dir
    job = Job(filename)
    Validator.validate(job)
    scheduler.generate_scripts(job)