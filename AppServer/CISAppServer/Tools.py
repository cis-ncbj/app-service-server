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

from logging import \
        debug, info, warning, error

class CTemplate(string.Template):
    """Class used to substitute placeholder strings in PBS script templates.
    The placehpolders have following form: @@{KEY}.
    """
    delimiter = '@@'


class Tools(object):
    """Collection of methods that are usefull to be shared between JobManager,
    Validator and PbsManager"""

    def generate_job_status(self, job, msg):
        """Generate status.txt file in job output directory. Dump msg into the
        file."""

        try:
            _dir = os.path.join(self.config['external_data_path'], job)
            os.mkdir(_dir)
        except IOError:
            error("Cannot create job output dir: %s"%_dir, exc_info=True)

        try:
            _name = os.path.join(_dir, 'status.txt')
            _f = open(_name, 'w')
            _f.write(msg)
            _f.close()
        except IOError:
            error("Cannot write to status file: %s"%_name, exc_info=True)

    def _rmtree_error(self, function, path, exc_info):
        """Exception handler for shutil.rmtree()."""

        error("Error: Cannot remove job output: %s %s"%(function, path),
                exc_info=exc_info)


class Validator(Tools):
    """Class responsible for loading, parsing and validation of job input data.
    It is also responsible for preparation of PBS scripts"""

    def __init__(self, config):
        """Ctor for Validator. Loads services configuration from input files.

        Arguments:
        config - config dictionary of JobManager
        """

        self.config = config
        self.jobs_dir = os.path.join(self.config['external_queue_path'], 'jobs')
        self.queue_dir = os.path.join(self.config['external_queue_path'], 'queue')
        self.done_dir = os.path.join(self.config['external_queue_path'], 'done')
        self.running_dir = os.path.join(self.config['external_queue_path'], 'running')
        self.delete_dir = os.path.join(self.config['external_queue_path'], 'delete')
        self.services = {}

        # Load all files from service_conf_path. Configuration files should be
        # in JSON format.
        debug('Loading services configurations ...')
        _path = self.config['service_conf_path']
        _services = os.listdir(_path)
        for _service in _services:
            #TODO validate options structure
            _file_name = os.path.join(_path, _service)
            # Allow for subdirectories
            if not os.path.isfile(_file_name):
                continue
            _f = open(_file_name)
            _data = json.load(_f)
            _f.close()
            self.services[_service] = _data
            info("Initialized service: %s"%_service)
        debug(json.dumps(self.services))


    def prepare_submission(self, job):
        """Load job description, validate input data and finally generate
        output PBS scripts.

        Arguments:
        job - Job ID

        Returns True on success False otherwise."""

        # Load job file from jobs directory
        try:
            _name = os.path.join(self.jobs_dir, job)
            _f = open(_name)
            _data = json.load(_f)
            _f.close()
            debug('Loaded job data: %s'%job)
            debug(_data)
        except:
            _msg = "Cannot load job data: %s."%_name
            error(_msg, exc_info=True)
            self.generate_job_status(job, "Error: " + _msg)
            return False

        # Check if data contains service attribute and that such service was
        # initialized
        if _data['service'] not in self.services.keys():
            _msg = "Not supported service: %s."%_data['service']
            warning(_msg)
            self.generate_job_status(job, "Error: " + _msg)
            return False

        # Load defaults
        # p >= 2.7
        # _variables = { _k: _v['default'] 
                # for _k, _v in self.services[_data['service']].items() } 
        # p2.5 workaround
        _variables = {}
        for _k, _v in self.services[_data['service']].items():
            _variables[_k] = _v['default']

        # Check that all attribute names are defined in service configuration
        # Validate values of the attributes
        for _k, _v in _data.items():
            if _k in _variables.keys():
                if not self.validate(_k, _v, _data['service']):
                    _msg = "Variable value not allowed: %s - %s."%(_k, _v)
                    warning(_msg)
                    self.generate_job_status(job, "Error: " + _msg)
                    return False
                else:
                    _variables[_k] = _v
                    debug("Value passed validation: %s - %s"%(_k, _v))
            # Check for possible reserved attribuet names like service, name, date
            elif _k not in self.config['reserved_keys']:
                _msg = "Not supported variable: %s."%_k
                warning(_msg)
                self.generate_job_status(job, "Error: " + _msg)
                return False

        # Run script generation
        if not self.generate_scripts(job, _variables, _data['service']):
            return False

        return True


    def validate(self, key, value, service):
        """Validate value for specified service attribute.

        Arguments:
        key - attribute name
        value - value to validate
        service - service for which attribute is defined

        Returns True on success False otherwise."""

        if self.services[service][key]['type'] == 'string':
            # Attribute of type string check the table of allowed values
            if not value in self.services[service][key]['values']:
                warning("Value not allowed: %s - %s."%(key, value))
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
                    raise ValueError("%s is not an int"%value)
                else:
                    _v = value
            except ValueError:
                warning('Value is not a proper int.', exc_info=True)
                return False

            # Check that atrribute value falls in allowed range
            try:
                if _v < self.services[service][key]['values'][0] or \
                        _v > self.services[service][key]['values'][1]:
                    warning("Value not in allowed range: %s - %s"%(key, _v))
                    return False
            except IndexError:
                error("Badly defined range for variable:  %s"%key)
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
                    raise ValueError("%s is not a float"%value)
                else:
                    _v = value
            except ValueError:
                warning('Value is not a proper float', exc_info=True)
                return False

            # Check that atrribute value falls in allowed range
            try:
                if _v < self.services[service][key]['values'][0] or \
                        _v > self.services[service][key]['values'][1]:
                    warning("Value not in allowed range: %s - %s"%(key, _v))
                    return False
            except IndexError:
                error("Validity range for variable %s not defined."%key)
                return False

        return True

    def generate_scripts(self, job, variables, service):
        """Generate scripts for PBS job from templates.

        Will walk through service_data_path/service and copy all files
        including recursion through subdirectories to PBS work directory for
        specified job (the directory structure is retained). For all files
        substitute all occurences of @@{atribute_name} with specified values.

        Arguments:
        job - Job ID
        variables - dictionary of attribute name and value pairs
        service - service for which scripts should be generated

        Returns True on success and False otherwise."""

        # Input directory
        _script_dir = os.path.join(self.config['service_data_path'], service)
        # Output directory
        _work_dir = os.path.join(self.config['pbs_work_path'], job)

        # Verify that input dir exists
        if not os.path.isdir(_script_dir):
            _msg = "Service data directory not found: %s."%_script_dir
            warning(_msg)
            self.generate_job_status(job, 'Error: ' + _msg)
            return False

        # Verify that input dir contains "pbs.sh" script
        if not os.path.isfile(os.path.join(_script_dir, 'pbs.sh')):
            _msg = "Missing \"pbs.sh\" script for service %s."%service
            warning(_msg)
            self.generate_job_status(job, 'Error: ' + _msg)
            return False

        # Create output dir
        if not os.path.isdir(_work_dir):
            try:
                os.mkdir(_work_dir)
            except IOError:
                _msg = "Cannot create PBS WORKDIR %s."%_work_dir
                warning(_msg)
                self.generate_job_status(job, 'Error: ' + _msg)
                return False

        # Recurse through input dir
        for _path, _dirs, _files in os.walk(_script_dir):
            # Relative paths for subdirectories
            _sub_dir = re.sub("^%s"%_script_dir, '', _path)
            debug("Sub dir: %s"%_sub_dir)
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
                except IOError:
                    _msg = "Cannot create job subdirectory %s."%_name
                    warning(_msg)
                    self.generate_job_status(job, 'Error: ' + _msg)
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
                        _line = _t.substitute(variables)
                        _fou.write(_line)
                    # Close files and make sure they have proper file attributes
                    _fin.close()
                    _fou.close()
                    # Copy file permisions
                    _st = os.stat(_fin_name)
                    os.chmod(_fou_name, _st.st_mode)
                except IOError:
                    _msg = "Scripts creation failed for job: %s."%job
                    warning(_msg, exc_info=True)
                    self.generate_job_status(job, 'Error: ' + _msg)
                    return False

        # Make sure that "pbs.sh" is executable
        try:
            _st = os.stat(os.path.join(_script_dir, 'pbs.sh'))
            os.chmod(os.path.join(_work_dir, 'pbs.sh'), _st.st_mode | stat.S_IXUSR)
        except IOError:
            _msg = "Unable to change permissions for pbs.sh: %s."%job
            warning(_msg)
            self.generate_job_status(job, 'Error: ' + _msg)
            return False

        return True


class PbsManager(Tools):
    """Class implementing simple interface for PBS queue system. Allows for job
    submission and extraction of job status."""

    #TODO Actual PBS interface implementattion :-p

    def __init__(self, config):
        """Ctor for PbsManager.

        Arguments:
        config - configuration dictionary from JobManager"""

        self.config = config
        self.jobs_dir = os.path.join(self.config['external_queue_path'], 'jobs')
        self.queue_dir = os.path.join(self.config['external_queue_path'], 'queue')
        self.done_dir = os.path.join(self.config['external_queue_path'], 'done')
        self.running_dir = os.path.join(self.config['external_queue_path'], 'running')
        self.delete_dir = os.path.join(self.config['external_queue_path'], 'delete')

        # DEBUG only
        self.debug_queue = {}

    def submit(self, job):
        """Submit a job to PBS queue. The "pbs.sh" script should be already
        present in the pbs_work_path/job directory.

        Arguments:
        job - Job ID

        Returns True on success and False otherwise."""

        f = open(os.path.join(self.config['pbs_queue_path'], job), 'w')
        f.close()
        info("Job successfully submitted: %s"%job)
        return True

    def status(self, job):
        """Return status of the job in PBS queue.

        Arguments:
        job - Job ID

        Returns one of "waiting", "running", "done"."""

        if job not in self.debug_queue.keys():
            self.debug_queue[job] = 1
        else:
            self.debug_queue[job] += 1

        if self.debug_queue[job] > 20:
            del self.debug_queue[job]
            return 'done'
        else:
            return 'running'

    def stop(self, job):
        """Stop running job and remove it from PBS queue."""
        return True

    def finalise(self, job):
        """Prepare output of finished job.

        Job working directory is moved to the external_data_path directory. A
        "status.txt" file is created with return value of the job.

        Arguments:
        job - Job ID

        Return True on success and False otherwise."""

        #TODO remove input data & scripts
        #TODO add creation of status.txt with job return value
        debug("Retrive job output: %s"%job)
        _work_dir = os.path.join(self.config['pbs_work_path'], job)
        try:
            os.unlink(os.path.join(self.config['pbs_queue_path'], job))
            # Output dir should not exist. For now there is no case where this
            # would happen ...
            # p >= 2.7
            # shutil.move(_work_dir, self.config['external_data_path'])
            # p2.5 workaround - shutil.move is broken
            os.system("mv %s %s"%(_work_dir, self.config['external_data_path']))
        # except OSError, shutil.Error: 
        except OSError:
            error("Unable to retrive job output directory %s"%_work_dir,
                    exc_info=True)

