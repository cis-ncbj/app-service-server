#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
try:
    import json
except:
    import simplejson as json
import string
import stat
import re
import shutil
#import time

from subprocess import Popen, PIPE, STDOUT
from logging import \
    debug, info, warning, error, log

from Config import conf, VERBOSE


def rmtree_error(function, path, exc_info):
    """Exception handler for shutil.rmtree()."""

    error("@rmtree - Cannot remove job output: %s %s" % (function, path),
          exc_info=exc_info)

def verbose(msg, exc_info=False):
    log(VERBOSE, msg, exc_info=exc_info)

class CTemplate(string.Template):
    """
    Class used to substitute placeholder strings in script templates.
    The placeholders have following form: @@{KEY}.
    """
    #: Delimiter identifying keywords.
    delimiter = '@@'


class Validator(object):
    """
    Class responsible for validation of job input data.
    It is also responsible for preparation of PBS scripts.
    """

    def __init__(self):
        """
        Upon initialisation load services configuration.
        """

        #: Services configurations
        self.services = {}

        # Load all files from service_conf_path. Configuration files should be
        # in JSON format.
        debug('@Validator - Loading services configurations.')
        _path = conf.service_path_conf
        _services = os.listdir(_path)
        for _service in _services:
            #TODO validate options structure
            _file_name = os.path.join(_path, _service)
            # Allow for subdirectories
            if not os.path.isfile(_file_name):
                continue
            with open(_file_name) as _f:
                _data = json.load(_f)
            self.services[_service] = _data
            info("Initialized service: %s" % _service)
        verbose(json.dumps(self.services))

    def validate(self, job):
        """
        Validate job input data and update :py:class:`Job` instance with
        validated data.

        :param job: :py:class:`Job` instance
        :rerurn: True on success False otherwise.
        """

        _data = job.data

        # Check if data contains service attribute and that such service was
        # initialized
        if 'service' not in _data.keys() or \
           _data['service'] not in self.services.keys() or \
           _data['service'] == 'default':
            job.die("@Validator - Not supported service: %s." %
                    _data['service'], err=False)
            return False

        # Load defaults
        _variables = {_k: _v['default']
                      for _k, _v in self.services[_data['service']].items()}
        _variables.update({_k: _v['default']
                      for _k, _v in self.services['default'].items()})

        # Check that all attribute names are defined in service configuration
        # Validate values of the attributes
        for _k, _v in _data.items():
            if _k in self.services[_data['service']].keys():
                if not self.validate_value(_k, _v, _data['service']):
                    job.die(
                        "@Validator - Variable value not allowed: %s - %s." %
                        (_k, _v), err=False
                    )
                    return False
                else:
                    _variables[_k] = _v
                    debug("@Validator - Value passed validation: %s - %s" %
                          (_k, _v))
            # Check for possible reserved attribuet names like service, name,
            # date
            elif _k in conf.service_reserved_keys:
                if not self.validate_value(_k, _v, 'default'):
                    job.die(
                        "@Validator - Variable value not allowed: %s - %s." %
                        (_k, _v), err=False
                    )
                    return False
                else:
                    _variables[_k] = _v
                    debug("@Validator - Value passed validation: %s - %s" %
                          (_k, _v))
            else:
                job.die("@Validator - Not supported variable: %s." % _k,
                        err=False)
                return False

        # Update job data with default values
        job.valid_data = _variables
        verbose('@Validator - Validated input data:')
        verbose(job.valid_data)

        return True

    def validate_value(self, key, value, service):
        """
        Validate value for specified service attribute.

        :param key: attribute name
        :param value: value to validate
        :param service: service name for which attribute is defined
        :return: True on success False otherwise.
        """

        if self.services[service][key]['type'] == 'string':
            # Attribute of type string check the table of allowed values
            if not value in self.services[service][key]['values']:
                warning("@Validator - Value not allowed: %s - %s." %
                        (key, value))
                return False
        elif self.services[service][key]['type'] == 'int':
            # Attribute of type int - check the format
            try:
                if isinstance(value, str) or isinstance(value, unicode):
                    # Value specified as string - check the format using python
                    # builtin conversion
                    _v = int(value)
                elif not isinstance(value, int):
                    # Value specified neither as int nor string - raise error
                    raise ValueError("%s is not an int" % value)
                else:
                    _v = value
            except ValueError:
                warning('@Validator - Value is not a proper int.',
                        exc_info=True)
                return False

            # Check that atrribute value falls in allowed range
            try:
                if _v < self.services[service][key]['values'][0] or \
                        _v > self.services[service][key]['values'][1]:
                    warning(
                        "@Validator - Value not in allowed range: %s - %s" %
                        (key, _v)
                    )
                    return False
            except IndexError:
                error("@Validator - Badly defined range for variable:  %s" %
                      key)
                return False
        elif self.services[service][key]['type'] == 'float':
            # Attribute of type float - check the format
            try:
                if isinstance(value, str) or isinstance(value, unicode):
                    # Value specified as string - check the format using python
                    # builtin conversion
                    _v = float(value)
                elif not isinstance(value, float):
                    # Value specified neither as float nor string - raise error
                    raise ValueError("%s is not a float" % value)
                else:
                    _v = value
            except ValueError:
                warning('@Validator - Value is not a proper float',
                        exc_info=True)
                return False

            # Check that atrribute value falls in allowed range
            try:
                if _v < self.services[service][key]['values'][0] or \
                        _v > self.services[service][key]['values'][1]:
                    warning(
                        "@Validator - Value not in allowed range: %s - %s" %
                        (key, _v)
                    )
                    return False
            except IndexError:
                error("@Validator - Badly defined range for variable:  %s" %
                      key)
                return False

        return True


class Scheduler(object):
    """
    Virtual Class implementing simple interface for execution backend. Actual
    execution backends should derive from this class and implement defined
    interface.

    Allows for job submission, deletion and extraction of job status.
    """

    def __init__(self):
        #: Working directory path
        self.work_path = None
        #: Path where submitted job IDs are stored
        self.queue_path = None
        #: Default queue
        self.default_queue = None
        #: Maximum number of concurent jobs
        self.max_jobs = None

    def submit(self, job):
        """
        Submit a job for execution. The "pbs.sh" script should be already
        present in the work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """
        raise NotImplementedError

    def status(self, job):
        """
        Return status of the job in execution queue.

        :param job: :py:class:`Job` instance

        Returns one of "waiting", "running", "done", "unknown".
        """
        raise NotImplementedError

    def stop(self, job):
        """Stop running job and remove it from execution queue."""
        raise NotImplementedError

    def finalise(self, job):
        """
        Prepare output of finished job.

        Job working directory is moved to the output_path directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """
        raise NotImplementedError

    def generate_scripts(self, job):
        """
        Generate scripts and job input files from templates.

        Will walk through service_data_path/service and copy all files
        including recursion through subdirectories to PBS work directory for
        specified job (the directory structure is retained). For all files
        substitute all occurences of @@{atribute_name} with specified values.

        :param job: :py:class:`Job` instance after validation
        :return: True on success and False otherwise.
        """

        # Input directory
        _script_dir = os.path.join(conf.service_path_data,
                                   job.valid_data['service'])
        # Output directory
        _work_dir = os.path.join(self.work_path, job.id)

        # Verify that input dir exists
        if not os.path.isdir(_script_dir):
            job.die("@Scheduler - Service data directory not found: %s." %
                    _script_dir)
            return False

        # Verify that input dir contains "pbs.sh" script
        if not os.path.isfile(os.path.join(_script_dir, 'pbs.sh')):
            job.die("@Scheduler - Missing \"pbs.sh\" script for service %s." %
                    job.valid_data['service'])
            return False

        # Create output dir
        if not os.path.isdir(_work_dir):
            try:
                os.mkdir(_work_dir)
            except IOError:
                job.die("@Scheduler - Unable to create WORKDIR %s." %
                        _work_dir, exc_info=True)
                return False

        # Recurse through input dir
        debug("@Scheduler - generate scripts")
        for _path, _dirs, _files in os.walk(_script_dir):
            # Relative paths for subdirectories
            _sub_dir = re.sub("^%s" % _script_dir, '', _path)
            debug("@Scheduler - Sub dir: %s" % _sub_dir)
            if _sub_dir:
                # Remove starting /
                _sub_dir = _sub_dir[1:]
                _out_dir = os.path.join(_work_dir, _sub_dir)
            else:
                _out_dir = _work_dir

            # Create subdirectories in output dir
            for _dir in _dirs:
                _name = os.path.join(_out_dir, _dir)
                try:
                    os.mkdir(_name)
                except:
                    job.die(
                        "@Scheduler - Cannot create job subdirectory %s." %
                        _name, exc_info=True
                    )
                    return False

            # Iterate through script files in current subdir
            for _file in _files:
                # Input and output file names
                _fin_name = os.path.join(_path, _file)
                _fou_name = os.path.join(_out_dir, _file)
                try:
                    # Open input template script and output file
                    _fin = open(_fin_name, 'r')
                    _fou = open(_fou_name, 'w')

                    # Loop through input lines and perform substitutions using
                    # string.Template module.
                    for _line in _fin:
                        # REGEXPS
                        _t = CTemplate(_line)
                        _line = _t.substitute(job.valid_data)
                        _fou.write(_line)
                    # Close files
                    _fin.close()
                    _fou.close()
                    # Copy file permisions
                    _st = os.stat(_fin_name)
                    os.chmod(_fou_name, _st.st_mode)
                except:
                    job.die(
                        "@Scheduler - Scripts creation failed for job: %s." %
                        job.id, exc_info=True
                    )
                    return False

        # Make sure that "pbs.sh" is executable
        try:
            _st = os.stat(os.path.join(_script_dir, 'pbs.sh'))
            os.chmod(os.path.join(_work_dir, 'pbs.sh'),
                     _st.st_mode | stat.S_IXUSR)
        except:
            job.die(
                "@Scheduler - Unable to change permissions for pbs.sh: %s." %
                job.id, exc_info=True
            )
            return False

        return True


class PbsScheduler(Scheduler):
    """
    Class implementing simple interface for PBS queue system.

    Allows for job submission, deletion and extraction of job status.
    """

    #TODO Actual PBS interface implementattion :-p

    def __init__(self):
        #: PBS Working directory path
        self.work_path = conf.pbs_path_work
        #: Path where submitted PBS job IDs are stored
        self.queue_path = conf.pbs_path_queue
        #: Default PBS queue
        self.default_queue = conf.pbs_default_queue
        #: Maximum number of concurent jobs
        self.max_jobs = conf.pbs_max_jobs

    def submit(self, job):
        """
        Submit a job to PBS queue. The "pbs.sh" script should be already
        present in the pbs_work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """

        # Path names
        _work_dir = os.path.join(self.work_path, job.id)
        _run_script = os.path.join(_work_dir, "pbs.sh")
        _output_log = os.path.join(_work_dir, "output.log")
        # Select queue
        _queue = self.default_queue
        if job.valid_data['queue'] != "":
            _queue = job.valid_data['queue']

        try:
            # Submit
            debug("@PBS - Submitting new job")
            _opts = ['/usr/bin/qsub', '-q', _queue,
                     '-d', _work_dir, '-j', 'oe', '-o',  _output_log,
                     '-l', 'epilogue=epilogue.sh', _run_script]
            debug("@PBS - Running command: %s" % str(_opts))
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()
            # Hopefully qsub returned meaningful job ID
            _jid = _output[0]
            # Check return code. If qsub was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/qsub returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            job.die("@PBS - Unable to submit job %s." % job.id, exc_info=True)
            return False

        # Store the PBS job ID into a file
        with open(os.path.join(self.queue_path, job.id), 'w') as _jid_file:
            _jid_file.write(_jid)
        info("Job successfully submitted: %s" % job.id)
        return True

    def status(self, job):
        """
        Return status of the job in PBS queue.

        :param job: :py:class:`Job` instance

        Returns one of "waiting", "running", "done", "unknown".
        """

        _done = 0
        _status = 'unknown'
        _pbs_id = ''
        _work_dir = os.path.join(self.work_path, job.id)
        try:
            with open(os.path.join(self.queue_path, job.id)) as _pbs_file:
                _pbs_id = _pbs_file.readline().strip()
        except:
            job.die('@PBS - Unable to read PBS job ID', exc_info=True)
            return _status

        try:
            # Run qstat
            debug("@PBS - Check job state")
            _opts = ["/usr/bin/qstat", "-f", _pbs_id]
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()[0]
            if _proc.returncode == 153:
                debug('@PBS - Job ID missing from PBS queue: done or error')
                _done = 1
            # Check return code. If qstat was not killed by signal Popen will
            # not rise an exception
            elif _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/qstat returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            error("@PBS - Unable to check job %s state." % job.id, exc_info=True)
            return _status

        if _done == 0:
            verbose('@PBS - Qstat returned meaningful output. Start parsing.')
            verbose(str(_output))
            _re = re.compile('^job_state = (.*)')
            for _line in _output.split('\n'):
                _m = _re.match(_line.strip())
                if _m is not None:
                    _res = _m.group(1)
                    debug("@PBS - Found job_state: %s" % _res)
                    try:
                        # Consider running, exiting and complete as running
                        if _res == 'R' or _res == 'C' or _res == 'E':
                            _status = 'running'
                            job.set_state('running')
                        # Other states are considered as queued
                        else:
                            _status = 'queued'
                            job.set_state('queued')
                    except:
                        job.die("@PBS - Unable to set state of job %s" %
                                job.id, exc_info=True)
                        return 'unknown'

        elif os.path.isfile(os.path.join(_work_dir, 'status.dat')):
            debug("@PBS - Found job state: D")
            _status = 'done'

        return _status

    def stop(self, job):
        """Stop running job and remove it from PBS queue."""
        return True

    def finalise(self, job):
        """
        Prepare output of finished job.

        Job working directory is moved to the external_data_path directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """

        #TODO remove input data & scripts
        #TODO add creation of status.txt with job return value
        debug("@PBS - Retrive job output: %s" % job.id)
        _work_dir = os.path.join(self.work_path, job.id)
        _status = 0

        # Get job output code
        try:
            with open(os.path.join(_work_dir, 'status.dat')) as _status_file:
                _status = int(_status_file.readline().strip())
        except:
            job.die("@PBS - Unable to extract job exit code: %s. "
                    "Will continue with output extraction" % job.id,
                    exc_info=True)
            # Although there is no output code job might finished only epilogue
            # failed. Let the extraction finish.

        try:
            os.unlink(os.path.join(self.queue_path, job.id))
            # Output dir should not exist. For now there is no case where this
            # would happen ...
            shutil.move(_work_dir, conf.gate_path_output)
        except:
            job.die("@PBS - Unable to retrive job output directory %s" %
                    _work_dir, exc_info=True)
            return False
        info("Job %s output retrived." % job.id)

        if _status == 0:
            job.exit("%d" % _status)
        else:
            job.exit("%d" % _status, state='failed')

        return True
