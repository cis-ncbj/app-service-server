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

from Config import conf, verbose, ExitCodes
from DataStore import Service, ServiceStore
from Jobs import JobData, JobState, JobChain, StateManager, SchedulerQueue

logger = logging.getLogger(__name__)


class CisError(Exception):
    """ Wrong job input. """


class ValidatorError(Exception):
    """ Wrong job input. """


class ValidatorInputFileError(Exception):
    """ Wrong job state exception. """


def rmtree_error(function, path, exc_info):
    """Exception handler for shutil.rmtree()."""

    logger.error("@rmtree - Cannot remove job output: %s %s" %
                 (function, path), exc_info=exc_info)


class CTemplate(string.Template):
    """
    Class used to substitute placeholder strings in script templates.
    The placeholders have following form: @@{KEY}.
    """
    #: Delimiter identifying keywords.
    delimiter = '@@'
    idpattern = '[_a-z0-9]+'


class Validator(object):
    """
    Class responsible for validation of job input data.
    It is also responsible for preparation of PBS scripts.
    """

    def __init__(self):
        """
        Upon initialisation load services configuration.
        """

        #: Min API level
        self.api_min = 1.0
        #: Max API level
        self.api_max = 2.0
        #: Current API level
        self.api_current = 2.0
        #: Current job instance
        #self.job = None
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
            ServiceStore[_service] = Service(_service, _data)

            # Plugin
            if 'plugin' in _data['config']:
                _plugin_dir = os.path.join(conf.service_path_data, _service)
                _plugin_dir = os.path.join(_plugin_dir, 'plugins')
                _plugins.append(_plugin_dir)

            logger.info("Initialized service: %s" % _service)
        verbose(json.dumps(ServiceStore))

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

        #self.job = job

        # Do not validate jobs for the second time. This will conserve
        # resources in case scheduler queue is full and we try to resubmit
        if job.data:
            return

        # Make sure job.service is defined
	job.status.service = 'default'
        job.status.scheduler = conf.config_default_scheduler

        # Load job file from jobs directory
        _name = os.path.join(conf.gate_path_jobs, job.id())
        with open(_name) as _f:
            _data = json.load(_f)
        logger.debug(u'@Job - Loaded data file %s.' % job.id())
        verbose(_data)

        # Check if data contains service attribute and that such service was
        # initialized
        if 'service' not in _data.keys() or \
           _data['service'] not in ServiceStore.keys() or \
           _data['service'] == 'default':
            raise ValidatorError("@Validator - Not supported service: %s." %
                    _data['service'])

        job.status.service = _data['service']
        _service = ServiceStore[_data['service']]

        # Make sure that input dictionary exists
        if 'input' not in _data.keys():
            _data['input'] = {}
        elif not isinstance(_data['input'], dict):
            raise ValidatorError("@Validator - 'input' section is not a dictionary")

        # Make sure API level is correct
        if 'api' not in _data.keys():
            raise ValidatorError("@Validator - Job did not specify API level.")
        if not self.validate_float('api', _data['api'],
                                   self.api_min, self.api_max):
            raise ValidatorError("@Validator - API level %s is not supported." %
                    _data['api'])
        elif float(_data['api'] < self.api_current):
            # Deprecated API requested. Mark as such
            job.set_flag(JobState.FLAG_OLD_API)

        # Make sure no unsupported sections were passed
        for _k in _data.keys():
            if _k not in conf.service_allowed_sections:
                raise ValidatorError("@Validator - Section '%s' is not allowed in job "
                        "definition." % _k)

        # Load defaults
        _variables = {
            _k: _v['default'] for _k, _v in
            _service.variables.items()
        }
        _variables.update({
            _k: _v['default'] for _k, _v in
            ServiceStore['default'].variables.items()
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
                    raise ValidatorError(
                        "@Validator - Set variables have to be of type int or "
                        "string. (%s: %s)" % (_k, _v)
                    )
                if _v != 1:
                    raise ValidatorError(
                        "@Validator - Set variables only accept value of 1. "
                        "(%s: %s)" % (_k, _v)
                    )
                _variables.update(
                    {_kk: _vv for _kk, _vv in
                     _service.sets[_k].items()}
                )
                del _data['input'][_k]

        # Load variables
        for _k, _v in _data['input'].items():
            if _k in conf.service_reserved_keys or _k.startswith('CIS_CHAIN'):
                raise ValidatorError("@Validator - '%s' variable name is restricted." % _k)
            elif _k in _service.variables.keys():
                _variables[_k] = _v
            else:
                raise ValidatorError("@Validator - Not supported variable: %s." % _k)

        # Check that all attribute names are defined in service configuration
        # Validate values of the attributes
        for _k, _v in _variables.items():
            if _k in _service.variables.keys():
                if not self.validate_value(_k, _v, _service):
                    raise ValidatorError(
                        "@Validator - Variable value not allowed: %s - %s." %
                        (_k, _v)
                    )
                else:
                    _variables[_k] = _v
                    logger.debug(
                        "@Validator - Value passed validation: %s - %s" %
                        (_k, _v)
                    )
            # Check for possible reserved attribuet names like service, name,
            # date
            elif _k in conf.service_reserved_keys:
                if not self.validate_value(_k, _v, ServiceStore['default']):
                    raise ValidatorError(
                        "@Validator - Variable value not allowed: %s - %s." %
                        (_k, _v)
                    )
                else:
                    _variables[_k] = _v
                    logger.debug(
                        "@Validator - Value passed validation: %s - %s" %
                        (_k, _v)
                    )
            else:
                raise ValidatorError("@Validator - Not supported variable: %s." % _k)

        # Validate job output chaining. Check if defined job IDs point to
        # existing jobs in 'done' state.
        if 'chain' in _data.keys():
            if not isinstance(_data['chain'], list) and \
               not isinstance(_data['chain'], tuple):
                raise ValidatorError("@Validator - 'chain' section is not a list")

            if not self.validate_chain(_data['chain']):
                raise ValidatorError("@Validator - Bad job chain IDs.")
            else:
                _chain = []
                # Generate keywords for script substitutions
                _i = 0
                for _id in _data['chain']:
                    _variables["CIS_CHAIN%s" % _i] = _id
                    _i += 1
                    _chain.append(JobChain(id=_id))
                logger.debug(
                    "@Validator - Job chain IDs passed validation: %s" %
                    _data['chain']
                )
                job.chain = _chain

        # Job scheduler
        if 'CIS_SCHEDULER' not in _variables.keys():
            raise ValidatorError("@Validator - Scheduler not defined for job %s." % job.id())
        job.status.scheduler = _variables['CIS_SCHEDULER']

        # Update job data with default values
        job.data = JobData(data=_variables)
        verbose('@Validator - Validated input data:')
        verbose(_variables)

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
        :throws:
        :throws:
        """
        # Extract input file name
        _input_dir = os.path.join(conf.gate_path_input, job.id())
        _input_file = os.path.join(_input_dir, key)
        # Check that input file is ready
        if not os.path.isfile(_input_file):
            raise ValidatorInputFileError("Missing input file %s." %
                                          _input_file)

        # Load plugin and run the validate method
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
        _finished = (_j.id() for _j in StateManager.get_job_list('done'))
        for _id in chain:
            # ID of type string check if it is listed among finished jobs
            if not _id in _finished:
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

    def progress(self, job):
        """
        Extract the job progress log and expose it to the user.

        :param job: :py:class:`Job` instance
        """
        # Output job progress log if it exists
        # The progres log is extraced every n-th status check
        if Scheduler.__progress_step >= conf.config_progress_step:
            verbose("@Scheduler - Extracting progress log (%s)" % job.id())
            _work_dir = os.path.join(self.work_path, job.id())
            _output_dir = os.path.join(conf.gate_path_output, job.id())
            _progress_file = os.path.join(_work_dir, 'progress.log')
            if os.path.exists(_progress_file):
                try:
                    if not os.path.isdir(_output_dir):
                        os.mkdir(_output_dir)
                    shutil.copy(_progress_file, _output_dir)
                    verbose('@Scheduler - Progress log extracted')
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

    def finalise(self, jid):
        """
        Prepare output of finished job.

        Job working directory is moved to the external_data_path directory.

        :param job: :py:class:`Job` instance
        """

        _clean = True

        logger.debug("@Scheduler - Retrive job output: %s" % jid)
        _work_dir = os.path.join(self.work_path, jid)

        try:
            _session = StateManager.new_session()
            _job = StateManager.get_job(jid, _session)
        except:
            logger.error("@Scheduler - Unable to connect to DB.",
                         exc_info=True)
            raise

        # Cleanup of the output
        try:
            for _chain in _job.chain:
                shutil.rmtree(os.path.join(_work_dir, _chain.id),
                              ignore_errors=True)
        except:
            logger.error("@Scheduler - Unable to clean up job output "
                         "directory %s" % _work_dir, exc_info=True)

        if os.path.isdir(_work_dir):
            try:
                # Remove output dir if it exists.
                _out_dir = os.path.join(conf.gate_path_output, jid)
                _dump_dir = os.path.join(conf.gate_path_dump, jid)
                if os.path.isdir(_out_dir):
                    logger.debug('@Scheduler - Remove existing output directory')
                    # out and dump should be on the same partition so that rename
                    # is used. This will make sure that processes reading from out
                    # will not cause rmtree to throw exceptions
                    shutil.move(_out_dir, _dump_dir)
                    shutil.rmtree(_dump_dir, ignore_errors=True)
                shutil.move(_work_dir, conf.gate_path_output)

                # Make sure all files in the output directory are world readable -
                # so that apache can actually serve them
                logger.debug('@Scheduler - Make output directory world readable')
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
                _job.die("@Scheduler - Unable to retrive job output directory %s" %
                        _work_dir, exc_info=True)
                _clean = False

        if not _clean:
            try:
                StateManager.commit(_session)
                _session.close()
            except:
                logger.error("@Scheduler - Unable to finalize session.",
                             exc_info=True)
            return

        # Remove job from scheduler queue if it was queued
        try:
            _job.scheduler = None
        except:
            logger.error("@Scheduler - Unable to remove job SchedulerQueue.",
                         exc_info=True)

        try:
            # Update service quota
            ServiceStore[_job.status.service].update_job(_job)
            # Set job exit state
            _job.exit()

            StateManager.commit(_session)
            _session.close()
        except:
            logger.error("@Scheduler - Unable to finalize cleanup.",
                         exc_info=True)
        logger.debug("@Scheduler - job finalised: %s" % jid)

    def abort(self, jid):
        """
        Cleanup aborted job.

        Removes working and output directories.

        :param job: :py:class:`Job` instance
        """

        logger.debug("@Scheduler - Cleanup job: %s" % jid)

        _work_dir = os.path.join(self.work_path, jid)
        _out_dir = os.path.join(conf.gate_path_output, jid)
        _clean = True

        # Remove output dir if it exists.
        if os.path.isdir(_out_dir):
            logger.debug('@Scheduler - Remove existing output directory')
            shutil.rmtree(_out_dir, ignore_errors=True)
        # Remove work dir if it exists.
        if os.path.isdir(_work_dir):
            logger.debug('@Scheduler - Remove working directory')
            shutil.rmtree(_work_dir, ignore_errors=True)

        if os.path.isdir(_out_dir):
            _clean = False
            logger.error("@Scheduler - Unable to remove job output directory: "
                         "%s" % jid, exc_info=True)
        if os.path.isdir(_work_dir):
            _clean = False
            logger.error("@Scheduler - Unable to remove job working "
                         "directory: %s" % jid, exc_info=True)
        if _clean:
            logger.info("Job %s cleaned directories." % jid)

        try:
            _session = StateManager.new_session()
            _job = StateManager.get_job(jid, _session)
        except:
            logger.error("@Scheduler - Unable to connect to DB.",
                         exc_info=True)
            if _session is not None:
                _session.close()
            return

        # Remove job from scheduler queue if it was queued
        try:
            _job.scheduler = None
        except:
            logger.error("@Scheduler - Unable to remove job SchedulerQueue.",
                         exc_info=True)

        logger.debug("@Scheduler - finalize cleanup: %s" % jid)
        try:
            # Update service quota
            ServiceStore[_job.status.service].update_job(_job)
            # Set job exit state
            _job.exit()

            StateManager.commit(_session)
            _session.close()
        except:
            logger.error("@Scheduler - Unable to finalize cleanup.",
                         exc_info=True)
        logger.debug("@Scheduler - job abort finished: %s" % jid)

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
        _script_dir = os.path.join(conf.service_path_data, job.status.service)
        # Output directory
        _work_dir = os.path.join(self.work_path, job.id())

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
                        _line = _t.substitute(job.data.data)
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
                        job.id(), exc_info=True
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
                job.id(), exc_info=True
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
                "%s." % job.id(), exc_info=True
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
        _work_dir = os.path.join(self.work_path, job.id())
        for _cjob in job.chain:
            _id = _cjob.id
            _input_dir = os.path.join(conf.gate_path_output, _id)
            _output_dir = os.path.join(_work_dir, _id)
            if os.path.exists(_input_dir):
                try:
                    shutil.copytree(_input_dir, _output_dir)
                    logger.debug(
                        "@Scheduler - Job %s output chained as input for "
                        "Job %s" % (_id, job.id()))
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

    def __init__(self):
        super(PbsScheduler, self).__init__()
        #: PBS Working directory path
        self.work_path = conf.pbs_path_work
        #: Path where submitted PBS job IDs are stored
        self.queue_path = conf.pbs_path_queue
        #: Default PBS queue
        self.default_queue = conf.pbs_default_queue
        #: Maximum number of concurent jobs
        self.max_jobs = conf.pbs_max_jobs
        #: Scheduler name
        self.name = "pbs"

    def submit(self, job):
        """
        Submit a job to PBS queue. The "pbs.sh" script should be already
        present in the pbs_work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """

        # Check that maximum job limit is not exceeded
        _job_count = StateManager.get_scheduler_count(self.name)
        if _job_count < 0:
            return False
        if _job_count >= self.max_jobs:
            return False

        # Path names
        _work_dir = os.path.join(self.work_path, job.id())
        _run_script = os.path.join(_work_dir, "pbs.sh")
        _output_log = os.path.join(_work_dir, "output.log")
        # Select queue
        _queue = self.default_queue
        if job.data.data['CIS_QUEUE'] != "":
            _queue = job.data.data['CIS_QUEUE']

        try:
            # Submit
            logger.debug("@PBS - Submitting new job")
            # Run qsub with proper user permissions
            #
            # @TODO
            # Implementation of dedicated users for each service
            # - The apprunner user currently does not allow to log in via ssh
            #  - either this limitation is lifted or the server will be run as
            #    another user that is allowed to perform "sudo su apprunner"
            #  - to execute sudo su a TTY is required
            #  - this can be cirumvented using "ssh -t -t"
            #   - A passwordless key is setup:
            #    - .ssh/authorized_keys entry contains from="127.0.0.1" to
            #      limit the scope of the key
            #    - .ssh/config contains following entries to auto select the
            #      correct key when connecting to the "local" host:
            #     Host local
            #         HostName 127.0.0.1
            #         Port 22
            #         IdentityFile ~/.ssh/id_dsa_local
            #  - using this will generate TTY warning messages that are
            #    suppressed only for OpenSSH 5.4 or newer. Hence the STDERR
            #    should not be mixed with STDIN in this case
            # - There still is a problem of file permisions:
            #  - Data management is run with service users permisions
            #  - Data is set to be group writable
            #  - We use ACLs
            # @TODO END
            #
            # _user = self.jm.services[job.service].config['username']
            # _comm = "/usr/bin/qsub -q %s -d %s -j oe -o %s " \
                    # "-l epilogue=epilogue.sh %s" % \
                    # (_queue, _work_dir, _output_log, _run_script)
            # _sudo = "/usr/bin/sudo /bin/su -c \"%s\" %s" % (_comm, _user)
            # _opts = ['/usr/bin/ssh', '-t', '-t', 'local', _sudo]
            _opts = ['/usr/bin/qsub', '-q', _queue, '-d', _work_dir, '-j',
                     'oe', '-o', _output_log, '-l', 'epilogue=epilogue.sh',
                     _run_script]
            verbose("@PBS - Running command: %s" % str(_opts))
            _proc = Popen(_opts, stdout=PIPE, stderr=PIPE)
            _out = _proc.communicate()
            _output = _out[0]
            verbose(_out)
            # Hopefully qsub returned meaningful job ID
            _pbs_id = _output.strip()
            # Check return code. If qsub was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/qsub returned non zero exit code.\n%s" %
                    str(_out)
                ))
        except:
            job.die("@PBS - Unable to submit job %s." % job.id(), exc_info=True)
            return False

        # Store the PBS job ID
        job.scheduler = SchedulerQueue(scheduler=self.name, id=_pbs_id, queue=_queue)
        # Reduce memory footprint
        job.compact()

        logger.info("Job successfully submitted: %s" % job.id())
        return True

    def update(self, jobs):
        """
        Update job states to match their current state in PBS queue.

        :param jobs: A list of Job instances for jobs to be updated.
        """
        # Extract list of user names associated to the jobs
        _users = []
        for _service in ServiceStore.values():
            if _service.config['username'] not in _users:
                _users.append(_service.config['username'])

        # We agregate the jobs by user. This way one qstat call per user is
        # required instead of on call per job
        _job_states = {}
        for _usr in _users:
            try:
                # Run qstat
                verbose("@PBS - Check jobs state for user %s" % _usr)
                _opts = ["/usr/bin/qstat", "-f", "-x", "-u", _usr]
                verbose("@PBS - Running command: %s" % str(_opts))
                _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
                _output = _proc.communicate()[0]
                verbose(_output)
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
                    _xstate = _xjob.find('job_state').text
                    _xexit = _xjob.find('exit_status')
                    if _xexit is not None:
                        _xexit = _xexit.text
                    _job_states[_xjid] = (_xstate, _xexit)
            except:
                logger.error("@PBS - Unable to parse qstat output.",
                             exc_info=True)
                return
            verbose(_job_states)

        # Iterate through jobs
        for _job in jobs:
            _pbs_id = str(job.scheduler.id)
            # Check if the job exists in the PBS
            if _pbs_id not in _job_states.keys():
                _job.die('@PBS - Job does not exist in the PBS')
            else:
                # Update job progress output
                self.progress(_job)
                _new_state = 'queued'
                _exit_code = 0
                _state = _job_states[_pbs_id]
                verbose("@PBS - Current job state: '%s' (%s)" %
                        (_state[0], _job.id()))
                # Job has finished. Check the exit code.
                if _state[0] == 'C':
                    _new_state = 'done'
                    _msg = 'Job finished succesfully'

                    _exit_code = _state[1]
                    if _exit_code is None:
                        _new_state = 'killed'
                        _msg = 'Job was killed by the scheduler'
                        _exit_code = ExitCodes.SchedulerKill

                    _exit_code = int(_exit_code)
                    if _exit_code > 256:
                        _new_state = 'killed'
                        _msg = 'Job was killed by the scheduler'
                    elif _exit_code > 128:
                        _new_state = 'killed'
                        _msg = 'Job was killed'
                    elif _exit_code > 0:
                        _new_state = 'failed'
                        _msg = 'Job finished with error code'

                    try:
                        _job.finish(_msg, _new_state, _exit_code)
                    except:
                        _job.die('@PBS - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id()), exc_info=True)
                # Job is running
                elif _state[0] == 'R' or _state[0] == 'E':
                    if _job.get_state() != 'running':
                        try:
                            _job.run()
                        except:
                            _job.die("@PBS - Unable to set job state "
                                     "(running : %s)" % _job.id(), exc_info=True)
                # Treat all other states as queued
                else:
                    if _job.get_state() != 'queued':
                        try:
                            _job.queue()
                        except:
                            _job.die("@PBS - Unable to set job state "
                                     "(queued : %s)" % _job.id(), exc_info=True)

    def stop(self, job, msg, exit_code):
        """
        Stop running job and remove it from PBS queue.

        :param job: :py:class:`Job` instance
        :param msg: Message that will be passed to the user
        """
        _pbs_id = ''

        # @TODO move to state manager
        # Get Job PBS ID
        try:
            _pbs_id = str(job.scheduler.id)
        except:
            job.die('@PBS - Unable to read PBS job ID', exc_info=True)
            return

        # Run qdel
        try:
            logger.debug("@PBS - Killing job")
            # Run qdel with proper user permissions
            # _user = self.jm.services[job.service].config['username']
            # _comm = "/usr/bin/qdel %s" % _pbs_id
            # _sudo = "/usr/bin/sudo /bin/su -c \"%s\" %s" % (_comm, _user)
            # _opts = ['/usr/bin/ssh', '-t', '-t', 'local', _sudo]
            _opts = ['/usr/bin/qdel', _pbs_id]
            verbose("@PBS - Running command: %s" % str(_opts))
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()[0]
            verbose(_output)
            # Check return code. If qdel was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/qdel returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            job.die("@PBS - Unable to terminate job %s." %
                    job.id(), exc_info=True)
            return

        # Mark as killed by user
        job.mark(msg, exit_code)


class SshScheduler(Scheduler):
    """
    Class implementing simple interface for job execution through SSH.

    Allows for job submission, deletion and extraction of job status.
    """

    def __init__(self):
        super(SshScheduler, self).__init__()
        #: SSH Working directory path
        self.work_path = conf.ssh_path_work
        #: Path where submitted SSH job IDs are stored
        self.queue_path = conf.ssh_path_queue
        #: Default SSH execute host
        self.default_queue = conf.ssh_default_queue
        #: Dict of maximum number of concurent jobs per execution host
        self.max_jobs = conf.ssh_max_jobs
        #: Scheduler name
        self.name = "ssh"

    def submit(self, job):
        """
        Submit a job to execution host via SSH queue. The "pbs.sh" script
        should be already present in the ssh_work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """

        # Select execution host
        _queue = self.default_queue
        if job.data.data['CIS_SSH_HOST'] != "":
            _queue = job.data.data['CIS_SSH_HOST']

        # Check that maximum job limit is not exceeded
        _job_count = StateManager.get_scheduler_count(self.name)
        if _job_count < 0:
            return False
        if _job_count >= self.max_jobs:
            return False

        # Path names
        _work_dir = os.path.join(self.work_path, job.id())
        _run_script = os.path.join(_work_dir, "pbs.sh")
        _output_log = os.path.join(_work_dir, "output.log")

        try:
            # Submit
            logger.debug("@SSH - Submitting new job")
            # Run qsub with proper user permissions
            _user = ServiceStore[job.status.service].config['username']
            _shsub = os.path.join(os.path.dirname(conf.daemon_path_installdir),
                                  "Scripts")
            _shsub = os.path.join(_shsub, "shsub")
            _comm = "%s -i %s -d %s -o %s %s" % \
                    (_shsub, job.id(), _work_dir, _output_log, _run_script)
            _opts = ["/usr/bin/ssh", "-x", "-l", _user, _queue, _comm]
            verbose("@SSH - Running command: %s" % str(_opts))
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()[0]
            verbose(_output)
            # Hopefully shsub returned meaningful job ID
            _ssh_id = _output.strip()
            # Check return code. If ssh was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "shsub returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            job.die("@SSH - Unable to submit job %s." % job.id(), exc_info=True)
            return False

        # Store the SSH job ID
        job.scheduler = SchedulerQueue(scheduler=self.name, id=_ssh_id, queue=_queue)
        # Reduce memory footprint
        job.compact()

        logger.info("Job successfully submitted: %s" % job.id())
        return True

    def update(self, jobs):
        """
        Update job states to match their current state on SSH execution host.

        :param jobs: A list of Job instances for jobs to be updated.
        """
        # Extract list of user names and queues associated to the jobs
        _users = {}
        for _job in jobs:
            _service = ServiceStore[_job.status.service]
            _usr = _service.config['username']
            if _usr not in _users.keys():
                _users[_usr] = []
            _queue = _job.scheduler.queue
            if _queue not in _users[_usr]:
                _users[_usr].append(_queue)

        # We agregate the jobs by user. This way one shstat call per user is
        # required instead of on call per job
        _job_states = {}
        for _usr, _queues in _users.items():
            for _queue in _queues:
                try:
                    # Run shtat
                    verbose("@SSH - Check jobs state for user %s @ %s" %
                                 (_usr, _queue))
                    _shstat = os.path.join(
                        os.path.dirname(conf.daemon_path_installdir),
                        "Scripts"
                    )
                    _shstat = os.path.join(_shstat, "shstat")
                    _opts = ["/usr/bin/ssh", "-x", "-l", _usr, _queue, _shstat]
                    verbose("@SSH - Running command: %s" % str(_opts))
                    _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
                    _output = _proc.communicate()[0]
                    verbose(_output)
                    # Check return code. If ssh was not killed by signal Popen
                    # will not rise an exception
                    if _proc.returncode != 0:
                        raise OSError((
                            _proc.returncode,
                            "shstat returned non zero exit code.\n%s" %
                            str(_output)
                        ))
                except:
                    logger.error("@SSH - Unable to check jobs state.",
                                 exc_info=True)
                    return

                for _line in _output.splitlines():
                    try:
                        _jid, _state = _line.split(" ")
                        _job_states[_jid] = _state
                    except:
                        logger.error("@SSH - Unable to parse shstat output.",
                                     exc_info=True)
                        return

        # Iterate through jobs
        for _job in jobs:
            _pid = str(_job.scheduler.id)
            # Check if the job exists on a SSH execution host
            if _pid not in _job_states.keys():
                _job.die('@SSH - Job does not exist on any of the SSH '
                         'execution hosts')
            else:
                # Update job progress output
                self.progress(_job)
                _state = int(_job_states[_pid])
                # Job has finished. Check the exit code.
                if _state >= 0:
                    _new_state = 'done'
                    _msg = 'Job finished succesfully'
                    if _state > 128:
                        _new_state = 'killed'
                        _msg = 'Job was killed by a signal'
                    elif _state > 0:
                        _new_state = 'failed'
                        _msg = 'Job finished with error code'
                    try:
                        _job.finish(_msg, _new_state, _state)
                    except:
                        _job.die('@SSH - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id()), exc_info=True)
                # Job is running
                elif _state == -1:
                    if _job.get_state() != 'running':
                        try:
                            _job.run()
                        except:
                            _job.die("@SSH - Unable to set job state "
                                     "(running : %s)" % _job.id(), exc_info=True)
                # Treat all other states as queued
                else:
                    _new_state = 'failed'
                    _msg = 'Job finished with unknown exit state'
                    try:
                        _job.finish(_msg, _new_state, _state)
                    except:
                        _job.die('@SSH - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id()), exc_info=True)

    def stop(self, job, msg, exit_code):
        """
        Stop running job and remove it from SSH queue.

        :param job: :py:class:`Job` instance
        :param msg: Message that will be passed to the user
        """
        _pid = ''

        # Get Job PID
        try:
            _pid = str(job.scheduler.id)
        except:
            job.die('@SSH - Unable to read PID', exc_info=True)
            return

        # Run qdel
        try:
            logger.debug("@SSH - Killing job")
            # Run qdel with proper user permissions
            _usr = ServiceStore[job.status.service].config['username']
            _queue = job.scheduler.queue
            _shdel = os.path.join(
                os.path.dirname(conf.daemon_path_installdir),
                "Scripts"
            )
            _shdel = os.path.join(_shdel, "shdel")
            _opts = ["/usr/bin/ssh", "-x", "-l", _usr, _queue, _shdel, _pid]
            verbose("@SSH - Running command: %s" % str(_opts))
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()[0]
            verbose(_output)
            # Check return code. If ssh was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "shdel returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            job.die("@SSH - Unable to terminate job %s." %
                    job.id(), exc_info=True)
            return

        # Mark as killed by user
        job.mark(msg, exit_code)
