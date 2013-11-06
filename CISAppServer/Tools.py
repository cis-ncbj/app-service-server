# -*- coding: UTF-8 -*-
"""
Utility classes used by CISAppServer: validation, communication with queue
managers, etc.
"""

# Plugins
# http://yapsy.sourceforge.net/
# http://stackoverflow.com/questions/5333128/yapsy-minimal-example

import os
import json
import csv
import xml.etree.cElementTree as ET
import string
import stat
import re
import shutil
import logging
#import time

from subprocess import Popen, PIPE, STDOUT
from yapsy.PluginManager import PluginManager

from Config import conf, VERBOSE, ExitCodes

logger = logging.getLogger(__name__)


def rmtree_error(function, path, exc_info):
    """Exception handler for shutil.rmtree()."""

    logger.error("@rmtree - Cannot remove job output: %s %s" %
                 (function, path), exc_info=exc_info)


def verbose(msg, exc_info=False):
    """
    Log message with VERBOSE log level.

    VERBOSE log level is higher than DEBUG and should be used for large debug
    messages, e.g. data dumps, output from subprocesses, etc.
    """
    logger.log(VERBOSE, msg, exc_info=exc_info)


class CTemplate(string.Template):
    """
    Class used to substitute placeholder strings in script templates.
    The placeholders have following form: @@{KEY}.
    """
    #: Delimiter identifying keywords.
    delimiter = '@@'
    idpattern = '[_a-z0-9]+'


class Service(dict):
    """
    Class implementing a Service.

    Stores Service configuration and monitors its state.
    """
    def __init__(self, data, *args, **kwargs):
        # Service is a dict. Make all the keys accessible as attributes while
        # retaining the dict API
        super(Service, self).__init__(*args, **kwargs)
        self.__dict__ = self

        #: Configuration options affecting service handling - like allowed
        #: diskspace and garbage collection
        self.config = {
            'min_lifetime': conf.service_min_lifetime,
            'max_lifetime': conf.service_max_lifetime,
            'max_runtime': conf.service_max_runtime,
            'max_jobs': conf.service_max_jobs,
            'quota': conf.service_quota,
            'job_size': conf.service_job_size,
            'username': conf.service_username
        }
        # Load settings from config file
        self.config.update(data['config'])
        #: Definitions of allowed variables
        self.variables = data['variables']
        #: Definitions of allowed variable sets
        self.sets = data['sets']
        #: Real space usage by service output files
        self.real_size = 0
        #: Projection of space usage by service output files
        self.current_size = 0
        self.__job_proxies = []
        self.__jobs = []

        # Covert quota and job_size to bytes from MB
        self.config['quota'] = self.config['quota'] * 1000000
        self.config['job_size'] = self.config['job_size'] * 1000000

    def add_job_proxy(self, job):
        if job.id in self.__job_proxies:
            logger.error("@Service - Job proxy already exists: %s" % job.id)
            return

        self.current_size += self.config['job_size']
        self.__job_proxies.append(job.id)

    def remove_job_proxy(self, job):
        if job.id in self.__job_proxies:
            self.__job_proxies.remove(job.id)
            self.current_size -= job.get_size()

    def update_job(self, job):
        if job.id in self.__jobs:
            logger.error("@Service - Job already exists: %s" % job.id)
            return
        self.__jobs.append(job.id)
        if job.id in self.__job_proxies:
            self.current_size -= self.config['job_size']
        else:
            self.__job_proxies.append(job.id)
        job.calculate_size()
        self.current_size += job.get_size()
        self.real_size += job.get_size()

    def remove_job(self, job):
        if job.id not in self.__jobs:
            logger.error("@Service - Job does not exist: %s" % job.id)
            return
        if job.id in self.__job_proxies:
            self.__job_proxies.remove(job.id)
            self.current_size -= job.get_size()
        self.__jobs.remove(job.id)
        self.real_size -= job.get_size()


class Validator(object):
    """
    Class responsible for validation of job input data.
    It is also responsible for preparation of PBS scripts.
    """

    def __init__(self, jm):
        """
        Upon initialisation load services configuration.
        """

        #: Min API level
        self.api_min = 1.0
        #: Max API level
        self.api_max = 1.0
        #: Services configurations
        self.services = {}
        #: JobManager instance
        self.jm = jm
        #: Current job instance
        self.job = None
        #: PluginManager instance
        self.pm = PluginManager()

        # Load all files from service_conf_path. Configuration files should be
        # in JSON format.
        logger.debug('@Validator - Loading services configurations.')
        _path = conf.service_path_conf
        _services = os.listdir(_path)
        _plugins = []
        for _service in _services:
            #TODO validate options structure
            _file_name = os.path.join(_path, _service)
            # Allow for subdirectories
            if not os.path.isfile(_file_name):
                continue
            with open(_file_name) as _f:
                _data = conf.json_load(_f)
            self.services[_service] = Service(_data)

            # Plugin
            if 'plugin' in _data['config']:
                _plugin_dir = os.path.join(conf.service_path_data, _service)
                _plugin_dir = os.path.join(_plugin_dir, 'plugins')
                _plugins.append(_plugin_dir)

            logger.info("Initialized service: %s" % _service)
        verbose(json.dumps(self.services))

        # Load plugins
        self.pm.setPluginPlaces(_plugins)
        self.pm.collectPlugins()

    def validate(self, job):
        """
        Validate job input data and update :py:class:`Job` instance with
        validated data.

        :param job: :py:class:`Job` instance
        :rerurn: True on success False otherwise.
        """

        #TODO should be cleaned - whole return thing makes it hard ...
        self.job = job

        # Do not validate jobs for the second time. This will conserve
        # resources in case shceduler queue is full and we try to resubmit
        if job.valid_data:
            return True

        if not job.data:
            job.die("@Validator - Empty data dictionary",
                    exit_code=ExitCodes.Validate)
            return False

        _data = job.data

        # Check if data contains service attribute and that such service was
        # initialized
        if 'service' not in _data.keys() or \
           _data['service'] not in self.services.keys() or \
           _data['service'] == 'default':
            job.die("@Validator - Not supported service: %s." %
                    _data['service'], err=False,
                    exit_code=ExitCodes.Validate)
            return False

        # Make sure that input dictionary exists
        if 'input' not in _data.keys():
            _data['input'] = {}
        elif not isinstance(_data['input'], dict):
            job.die("@Validator - 'input' section is not a dictionary",
                    err=False, exit_code=ExitCodes.Validate)
            return False

        # Make sure API level is correct
        if 'api' not in _data.keys():
            job.die("@Validator - Job did not specify API level.", err=False,
                    exit_code=ExitCodes.Validate)
            return False
        if not self.validate_float('api', _data['api'],
                                   self.api_min, self.api_max):
            job.die("@Validator - API level %s is not supported." %
                    _data['api'], err=False, exit_code=ExitCodes.Validate)
            return False

        # Make sure no unsupported sections were passed
        for _k in _data.keys():
            if _k not in conf.service_allowed_sections:
                job.die("@Validator - Section '%s' is not allowed in job "
                        "definition." % _k, err=False,
                        exit_code=ExitCodes.Validate)
                return False

        job.service = _data['service']
        _service = self.services[_data['service']]

        # Load defaults
        _variables = {
            _k: _v['default'] for _k, _v in
            _service.variables.items()
        }
        _variables.update({
            _k: _v['default'] for _k, _v in
            self.services['default'].variables.items()
        })

        # Load sets
        for _k, _v in _data['input'].items():
            if _k in _service.sets.keys():
                if isinstance(_v, str) or isinstance(_v, unicode):
                    # Value specified as string - check the format using python
                    # builtin conversion
                    _v = int(_v)
                elif not isinstance(_v, int):
                    # Value specified neither as int nor string - raise error
                    job.die(
                        "@Validator - Set variables have to be of type int or "
                        "string. (%s: %s)" % (_k, _v),
                        err=False, exit_code=ExitCodes.Validate
                    )
                    return False
                if _v != 1:
                    job.die(
                        "@Validator - Set variables only accept value of 1. "
                        "(%s: %s)" % (_k, _v),
                        err=False, exit_code=ExitCodes.Validate
                    )
                    return False
                _variables.update(
                    {_kk: _vv for _kk, _vv in
                     _service.sets[_k].items()}
                )
                del _data['input'][_k]

        # Load variables
        for _k, _v in _data['input'].items():
            if _k in conf.service_reserved_keys or _k.startswith('CIS_CHAIN'):
                job.die("@Validator - '%s' variable name is restricted." % _k,
                        err=False, exit_code=ExitCodes.Validate)
                return False
            elif _k in _service.variables.keys():
                _variables[_k] = _v
            else:
                job.die("@Validator - Not supported variable: %s." % _k,
                        err=False, exit_code=ExitCodes.Validate)
                return False

        # Check that all attribute names are defined in service configuration
        # Validate values of the attributes
        for _k, _v in _variables.items():
            if _k in _service.variables.keys():
                if not self.validate_value(_k, _v, _service):
                    job.die(
                        "@Validator - Variable value not allowed: %s - %s." %
                        (_k, _v), err=False, exit_code=ExitCodes.Validate
                    )
                    return False
                else:
                    _variables[_k] = _v
                    logger.debug(
                        "@Validator - Value passed validation: %s - %s" %
                        (_k, _v)
                    )
            # Check for possible reserved attribuet names like service, name,
            # date
            elif _k in conf.service_reserved_keys:
                if not self.validate_value(_k, _v, self.services['default']):
                    job.die(
                        "@Validator - Variable value not allowed: %s - %s." %
                        (_k, _v), err=False, exit_code=ExitCodes.Validate
                    )
                    return False
                else:
                    _variables[_k] = _v
                    logger.debug(
                        "@Validator - Value passed validation: %s - %s" %
                        (_k, _v)
                    )
            else:
                job.die("@Validator - Not supported variable: %s." % _k,
                        err=False, exit_code=ExitCodes.Validate)
                return False

        # Validate job output chaining. Check if defined job IDs point to
        # existing jobs in 'done' state.
        if 'chain' in _data.keys():
            if not isinstance(_data['chain'], list) and \
               not isinstance(_data['chain'], tuple):
                job.die("@Validator - 'chain' section is not a list",
                        err=False, exit_code=ExitCodes.Validate)
                return False

            if not self.validate_chain(_data['chain']):
                job.die("@Validator - Bad job chain IDs.", err=False,
                        exit_code=ExitCodes.Validate)
                return False
            else:
                job.chain = _data['chain']
                # Generate keywords for script substitutions
                _i = 0
                for _id in job.chain:
                    _variables["CIS_CHAIN%s" % _i] = _id
                    _i += 1
                logger.debug(
                    "@Validator - Job chain IDs passed validation: %s" %
                    _data['chain']
                )

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
        :param service: service object for which attribute is defined
        :return: True on success False otherwise.
        """

        if service.variables[key]['type'] == 'string':
            # Attribute of type string check the table of allowed values
            if not value in service.variables[key]['values']:
                logger.warning("@Validator - Value not allowed: %s - %s." %
                               (key, value))
                return False
        elif service.variables[key]['type'] == 'int':
            try:
                return self.validate_int(
                    key, value,
                    service.variables[key]['values'][0],
                    service.variables[key]['values'][1]
                )
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
        elif service.variables[key]['type'] == 'float':
            try:
                return self.validate_float(
                    key, value,
                    service.variables[key]['values'][0],
                    service.variables[key]['values'][1]
                )
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
        elif service.variables[key]['type'] == 'string_array':
            if not isinstance(value, list) and isinstance(value, tuple):
                logger.error(
                    "@Validator - Value is not a proper array:  %s" % key
                )
                return False
            try:
                if len(value) > service.variables[key]['values'][0]:
                    logger.error(
                        "@Validator - Array  exceeds allowed length:  %s" % key
                    )
                    return False
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            for _v in value:
                if not _v in service.variables[key]['values'][1:]:
                    logger.warning("@Validator - Value not allowed: %s - %s." %
                                   (key, value))
                    return False
        elif service.variables[key]['type'] == 'int_array':
            if not isinstance(value, list) and isinstance(value, tuple):
                logger.error(
                    "@Validator - Value is not a proper array:  %s" % key
                )
                return False
            try:
                if len(value) > service.variables[key]['values'][0]:
                    logger.error(
                        "@Validator - Array  exceeds allowed length:  %s" % key
                    )
                    return False
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            _min = 0
            _max = 0
            try:
                _min = service.variables[key]['values'][1]
                _max = service.variables[key]['values'][2]
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            for _v in value:
                if not self.validate_int(key, _v, _min, _max):
                    return False
        elif service.variables[key]['type'] == 'float_array':
            if not isinstance(value, list) and isinstance(value, tuple):
                logger.error(
                    "@Validator - Value is not a proper array:  %s" % key
                )
                return False
            try:
                if len(value) > service.variables[key]['values'][0]:
                    logger.error(
                        "@Validator - Array  exceeds allowed length:  %s" % key
                    )
                    return False
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            _min = 0
            _max = 0
            try:
                _min = service.variables[key]['values'][1]
                _max = service.variables[key]['values'][2]
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            for _v in value:
                if not self.validate_float(key, _v, _min, _max):
                    return False

        return True

    def validate_file(self, key, job, service):
        """
        :throws: Lots of stuff
        """
        _input_dir = os.path.join(conf.gate_path_input, job.id)
        _input_file = os.path.join(_input_dir, key)
        _plugin = self.pm.getPluginByName(service.variables[key]['plugin'],
                                          service.name)
        _plugin.plugin_object.validate(_input_file)

    def validate_file_csv(self, name, type, min, max):
        _f = open(name)
        _csv = csv.reader(_f)
        for _v in _csv:
            if type == 'int':
                if not self.validate_int("CSV element", _v, min, max):
                    return False
            if type == 'float':
                if not self.validate_float("CSV element", _v, min, max):
                    return False

    def validate_int(self, key, value, min, max):
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
            logger.warning('@Validator - Value is not a proper int.',
                           exc_info=True)
            return False

        # Check that atrribute value falls in allowed range
        if _v < min or _v > max:
            logger.warning(
                "@Validator - Value not in allowed range: %s - %s" %
                (key, _v)
            )
            return False

        return True

    def validate_float(self, key, value, min, max):
        # Attribute of type float - check the format
        try:
            if isinstance(value, str) or isinstance(value, unicode):
                # Value specified as string - check the format using python
                # builtin conversion
                _v = float(value)
            elif not isinstance(value, float) and not isinstance(value, int):
                # Value specified neither as float nor string - raise error
                raise ValueError("%s is not a float" % value)
            else:
                _v = value
        except ValueError:
            logger.warning('@Validator - Value is not a proper float',
                           exc_info=True)
            return False

        # Check that atrribute value falls in allowed range
        if _v < min or _v > max:
            logger.warning(
                "@Validator - Value not in allowed range: %s - %s" %
                (key, _v)
            )
            return False

        return True

    def validate_chain(self, chain):
        for _id in chain:
            # ID of type string check if it is listed among finished jobs
            if not _id in self.jm.get_job_ids('done'):
                logger.warning(
                    "@Validator - Job %s did not finish or does not exist. "
                    "Unable to chain output." %
                    _id)
                return False
        return True


class Scheduler(object):
    """
    Virtual Class implementing simple interface for execution backend. Actual
    execution backends should derive from this class and implement defined
    interface.

    Allows for job submission, deletion and extraction of job status.
    """

    # We do not want to output progress with every status check. To lighten the
    # burden lets do it every n-th step
    __progress_step = 0

    def __init__(self, jm):
        #: Working directory path
        self.work_path = None
        #: Path where submitted job IDs are stored
        self.queue_path = None
        #: Default queue
        self.default_queue = None
        #: Maximum number of concurent jobs
        self.max_jobs = None
        #: JobManager instance
        self.jm = jm

    def submit(self, job):
        """
        Submit a job for execution. The "pbs.sh" script should be already
        present in the work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """
        raise NotImplementedError

    def progress(self, job):
        """
        Extract the job progress log and expose it to the user.

        :param job: :py:class:`Job` instance
        """
        # Output job progress log if it exists
        # The progres log is extraced every n-th status check
        if Scheduler.__progress_step >= conf.config_progress_step:
            logger.debug('@Scheduler - Extracting progress log')
            _work_dir = os.path.join(self.work_path, job.id)
            _output_dir = os.path.join(conf.gate_path_output, job.id)
            _progress_file = os.path.join(_work_dir, 'progress.log')
            if os.path.exists(_progress_file):
                try:
                    if not os.path.isdir(_output_dir):
                        os.mkdir(_output_dir)
                    shutil.copy(_progress_file, _output_dir)
                    logger.debug('@Scheduler - Progress log extracted')
                except:
                    logger.error(
                        '@Scheduler - Cannot copy progress.log',
                        exc_info=True
                    )
            Scheduler.__progress_step = 0
        else:
            Scheduler.__progress_step += 1

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
        # Make sure all files in the output directory are world readable - so
        # that apache can actually serve them
        try:
            _out_dir = os.path.join(conf.gate_path_output, job.id)
            if os.path.isdir(_out_dir):
                logger.debug(
                    '@Scheduler - Make output directory world readable')
                os.chmod(_out_dir, os.stat(_out_dir).st_mode |
                         stat.S_IRUSR | stat.S_IXUSR |
                         stat.S_IRGRP | stat.S_IXGRP |
                         stat.S_IROTH | stat.S_IXOTH)
                for _root, _dirs, _files in os.walk(_out_dir):
                    for _dir in _dirs:
                        _name = os.path.join(_root, _dir)
                        os.chmod(_name, os.stat(_name).st_mode |
                                 stat.S_IRUSR | stat.S_IXUSR |
                                 stat.S_IRGRP | stat.S_IXGRP |
                                 stat.S_IROTH | stat.S_IXOTH)
                    for _file in _files:
                        _name = os.path.join(_root, _file)
                        os.chmod(_name, os.stat(_name).st_mode |
                                 stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        except:
            job.die("@Scheduler - Unable to correct permissions for job "
                    "output directory %s" % _out_dir, exc_info=True)

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
        _script_dir = os.path.join(conf.service_path_data, job.service)
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
                    job.service)
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
        logger.debug("@Scheduler - generate scripts")
        for _path, _dirs, _files in os.walk(_script_dir):
            # Relative paths for subdirectories
            _sub_dir = re.sub("^%s" % _script_dir, '', _path)
            logger.debug("@Scheduler - Sub dir: %s" % _sub_dir)
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
                # Skip editor buffers and recovery files
                if _file.endswith('~'):
                    continue
                if _file.startswith('.') and _file.endswith('.swp'):
                    continue
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

        # Make sure that "epilogue.sh" is executable
        # Make sure it is not writable by group and others - torque will
        # silently ignore it otherwise
        try:
            _st = os.stat(os.path.join(_script_dir, 'epilogue.sh'))
            os.chmod(os.path.join(_work_dir, 'epilogue.sh'),
                     (_st.st_mode | stat.S_IXUSR) &
                     (~stat.S_IWGRP & ~stat.S_IWOTH))
        except:
            job.die(
                "@Scheduler - Unable to change permissions for epilogue.sh: "
                "%s." % job.id, exc_info=True
            )
            return False

        return True

    def chain_input_data(self, job):
        """
        Chain output data of finished jobs as our input.

        Copies output data of specified job IDs into the working directory of
        current job.

        :param job: :py:class:`Job` instance after validation
        :return: True on success and False otherwise.
        """
        logger.debug('@Scheduler - Chaining input data')
        _work_dir = os.path.join(self.work_path, job.id)
        for _id in job.chain:
            _input_dir = os.path.join(conf.gate_path_output, _id)
            _output_dir = os.path.join(_work_dir, _id)
            if os.path.exists(_input_dir):
                try:
                    shutil.copytree(_input_dir, _output_dir)
                    logger.debug(
                        "@Scheduler - Job %s output chained as input for "
                        "Job %s" % (_id, job.id))
                except:
                    job.die(
                        '@Scheduler - Cannot chain job %s output.' % _id,
                        err=True, exc_info=True
                    )
                    return False
            else:
                job.die(
                    '@Scheduler - Job %s output directory does not exists.' %
                    _id
                )
                return False

        return True


class PbsScheduler(Scheduler):
    """
    Class implementing simple interface for PBS queue system.

    Allows for job submission, deletion and extraction of job status.
    """

    def __init__(self, jm):
        super(PbsScheduler, self).__init__(jm)
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

        # Check that maximum job limit is not exceeded
        try:
            _queue = os.listdir(self.queue_path)
        except:
            logger.error("@PBS - unable to read queue directory %s." %
                         self.queue_path, exc_info=True)
            return False
        if len(_queue) >= self.max_jobs:
            return False

        # Path names
        _work_dir = os.path.join(self.work_path, job.id)
        _run_script = os.path.join(_work_dir, "pbs.sh")
        _output_log = os.path.join(_work_dir, "output.log")
        # Select queue
        _queue = self.default_queue
        if job.valid_data['CIS_QUEUE'] != "":
            _queue = job.valid_data['CIS_QUEUE']

        try:
            # Submit
            logger.debug("@PBS - Submitting new job")
            # Run qsub with proper user permissions
            _user = job.service.config['username']
            _comm = "/usr/bin/qsub -q %s -d %s -j oe -o %s " \
                    "-l epilogue=epilogue.sh %s" % \
                    (_queue, _work_dir, _output_log, _run_script)
            _opts = ["/bin/su", "-c", _comm, _user]
            logger.debug("@PBS - Running command: %s" % str(_opts))
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
        logger.info("Job successfully submitted: %s" % job.id)
        return True

    def update(self, jobs):
        """
        Update job states to match their current state in PBS queue.

        :param jobs: A list of Job instances for jobs to be updated.
        """
        # Extract list of user names associated to the jobs
        _users = []
        for _service in self.jm.services.values():
            if _service.config['username'] not in _users:
                _users.append(_service.config['username'])

        # We agregate the jobs by user. This way one qstat call per user is
        # required instead of on call per job
        _job_states = {}
        for _usr in _users:
            try:
                # Run qstat
                logger.debug("@PBS - Check jobs state for user %s" % _usr)
                _opts = ["/usr/bin/qstat", "-f", "-x", "-u", _usr]
                _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
                _output = _proc.communicate()[0]
                # Check return code. If qstat was not killed by signal Popen
                # will not rise an exception
                if _proc.returncode != 0:
                    raise OSError((
                        _proc.returncode,
                        "/usr/bin/qstat returned non zero exit code.\n%s" %
                        str(_output)
                    ))
            except:
                logger.error("@PBS - Unable to check jobs state.",
                             exc_info=True)
                return

            # Parse the XML output of qstat
            try:
                _xroot = ET.fromstring(_output)
                for _xjob in _xroot.iter('Job'):
                    _xjid = _xjob.find('Job_Id').text
                    _xstate = _xjob.get('job_state')
                    _xexit = _xjob.get('exit_status')
                    if _xexit is not None:
                        _xexit = _xexit.text
                    _job_states[_xjid] = (_xstate, _xexit)
            except:
                logger.error("@PBS - Unable to parse qstat output.",
                             exc_info=True)
                return

        # Iterate through jobs
        for _job in jobs:
            # Extract PBS id for the job
            _pbs_id = ''
            try:
                with open(os.path.join(self.queue_path, _job.id)) as _pbs_file:
                    _pbs_id = _pbs_file.readline().strip()
            except:
                _job.die('@PBS - Unable to read PBS job ID', exc_info=True)

            # Check if the job exists in the PBS
            if _pbs_id not in _job_states.keys():
                _job.die('@PBS - Job does not exist in the PBS')
            else:
                # Update job progress output
                self.progress(_job)
                _new_state = 'queued'
                _exit_code = 0
                _state = _job_states[_pbs_id]
                # Job has finished. Check the exit code.
                if _state[0] == 'C':
                    _new_state = 'done'
                    _msg = 'Job finished succesfully'
                    _exit_code = _state[1]
                    if _exit_code is None or int(_exit_code) > 256:
                        _new_state = 'killed'
                        _msg = 'Job was killed by the scheduler'
                    elif int(_exit_code) > 0:
                        _new_state = 'failed'
                        _msg = 'Job finished with error code'
                    try:
                        _job.finish(_msg, _new_state, _exit_code)
                    except:
                        _job.die('@PBS - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id), exc_info=True)
                # Job is running
                if _state[0] == 'R' or _state[0] == 'E':
                    if _job.get_state() != 'running':
                        try:
                            _job.run()
                        except:
                            _job.die("@PBS - Unable to set job state "
                                     "(running : %s)" % _job.id, exc_info=True)
                # Treat all other states as queued
                else:
                    if _job.get_state() != 'queued':
                        try:
                            _job.queue()
                        except:
                            _job.die("@PBS - Unable to set job state "
                                     "(queued : %s)" % _job.id, exc_info=True)

    def stop(self, job, msg, exit_code):
        """
        Stop running job and remove it from PBS queue.

        :param job: :py:class:`Job` instance
        :param msg: Message that will be passed to the user
        """
        _pbs_id = ''

        # Get Job PBS ID
        try:
            with open(os.path.join(self.queue_path, job.id)) as _pbs_file:
                _pbs_id = _pbs_file.readline().strip()
        except:
            job.die('@PBS - Unable to read PBS job ID', exc_info=True)
            return

        # Run qdel
        try:
            logger.debug("@PBS - Killing job")
            # Run qdel with proper user permissions
            _user = job.service.config['username']
            _opts = ["/bin/su", "-c", "/usr/bin/qdel %s" % _pbs_id, _user]
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()[0]
            # Check return code. If qstat was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/qdel returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            job.die("@PBS - Unable to terminate job %s." %
                    job.id, exc_info=True)
            return

        # Mark as killed by user
        job.mark(msg, exit_code)

    def finalise(self, job):
        """
        Prepare output of finished job.

        Job working directory is moved to the external_data_path directory.

        :param job: :py:class:`Job` instance
        """

        # Mark job as in cleanup state
        try:
            job.cleanup()
        except:
            job.die("@PBS - Unable to set state for job %s." % job.id)
            return

        logger.debug("@PBS - Retrive job output: %s" % job.id)
        _work_dir = os.path.join(self.work_path, job.id)

        # Cleanup of the output
        try:
            os.unlink(os.path.join(self.queue_path, job.id))
            for _chain in job.chain:
                shutil.rmtree(os.path.join(_work_dir, _chain),
                              ignore_errors=True)
        except:
            logger.error("@PBS - Unable to clean up job output directory %s" %
                         _work_dir, exc_info=True)

        try:
            # Remove output dir if it exists.
            _out_dir = os.path.join(conf.gate_path_output, job.id)
            _dump_dir = os.path.join(conf.gate_path_dump, job.id)
            if os.path.isdir(_out_dir):
                logger.debug('@PBS - Remove existing output directory')
                # out and dump should be on the same partition so that rename
                # is used. This will make sure that processes reading from out
                # will not cause rmtree to throw exceptions
                shutil.move(_out_dir, _dump_dir)
                shutil.rmtree(_dump_dir, ignore_errors=True)
            shutil.move(_work_dir, conf.gate_path_output)
            super(PbsScheduler, self).finalise(job)
            logger.info("Job %s output retrived." % job.id)
        except:
            job.die("@PBS - Unable to retrive job output directory %s" %
                    _work_dir, exc_info=True)
            return

        # Set job exit state
        job.exit()

    def abort(self, job):
        """
        Cleanup aborted job.

        Removes working and output directories.

        :param job: :py:class:`Job` instance
        """

        # Mark job as in cleanup state
        try:
            job.cleanup()
        except:
            job.die("@PBS - Unable to set state for job %s." % job.id)
            return

        logger.debug("@PBS - Cleanup job: %s" % job.id)

        _work_dir = os.path.join(self.work_path, job.id)
        _out_dir = os.path.join(conf.gate_path_output, job.id)

        # Remove output dir if it exists.
        if os.path.isdir(_out_dir):
            logger.debug('@PBS - Remove existing output directory')
            shutil.rmtree(_out_dir, ignore_errors=True)
        # Remove work dir if it exists.
        if os.path.isdir(_work_dir):
            logger.debug('@PBS - Remove working directory')
            shutil.rmtree(_work_dir, ignore_errors=True)

        if os.path.isdir(_out_dir):
            logger.error("@PBS - Unable to remove job output directory: %s" %
                         job.id, exc_info=True)
        if os.path.isdir(_out_dir):
            logger.error("@PBS - Unable to remove job working directory: %s" %
                         job.id, exc_info=True)
        else:
            logger.info("Job %s cleaned." % job.id)

        # Set job exit state
        job.exit()
