# -*- coding: UTF-8 -*-
"""
"""

# Plugins
# http://yapsy.sourceforge.net/
# http://stackoverflow.com/questions/5333128/yapsy-minimal-example

import os
import json
import csv
import string
import logging
from datetime import datetime

from yapsy.PluginManager import PluginManager

import Jobs  # Import full module - resolves circular dependencies
from Config import conf, VERBOSE

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
        Initialize defaults.
        """

        #: Min API level
        self.api_min = 1.0
        #: Max API level
        self.api_max = 2.0
        #: Current API level
        self.api_current = 2.0
        #: Current job instance
        # self.job = None
        #: PluginManager instance
        self.pm = PluginManager()

    def init(self):
        """
        Initialize Validator singleton.
        """

        _plugins = []
        # @TODO What about shared plugins??
        for _service_name, _service in ServiceStore.items():
            # Plugin
            if _service.plugins is not None:
                _plugin_dir = os.path.join(conf.service_path_data, _service_name)
                _plugin_dir = os.path.join(_plugin_dir, 'plugins')
                _plugins.append(_plugin_dir)

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

        # self.job = job

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
        logger.debug(u'@Job - Loaded data file %s.', job.id())
        logger.log(VERBOSE, _data)

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
            raise ValidatorError(
                "@Validator - 'input' section is not a dictionary")

        # Make sure API level is correct
        if 'api' not in _data.keys():
            raise ValidatorError("@Validator - Job did not specify API level.")
        if not self.validate_float('api', _data['api'],
                                   self.api_min, self.api_max):
            raise ValidatorError("@Validator - API level %s is not supported." %
                                 _data['api'])
        elif float(_data['api']) < self.api_current:
            # Deprecated API requested. Mark as such
            job.set_flag(Jobs.JobState.FLAG_OLD_API)

        # Make sure no unsupported sections were passed
        for _k in _data.keys():
            if _k not in conf.service_allowed_sections:
                raise ValidatorError(
                    "@Validator - Section '%s' is not allowed in job "
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
                raise ValidatorError(
                    "@Validator - '%s' variable name is restricted." % _k)
            elif _k in _service.variables.keys():
                _variables[_k] = _v
            else:
                raise ValidatorError(
                    "@Validator - Not supported variable: %s." % _k)

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
                raise ValidatorError(
                    "@Validator - Not supported variable: %s." % _k)

        # Validate job output chaining. Check if defined job IDs point to
        # existing jobs in 'done' state.
        if 'chain' in _data.keys():
            if not isinstance(_data['chain'], list) and \
                    not isinstance(_data['chain'], tuple):
                raise ValidatorError(
                    "@Validator - 'chain' section is not a list")

            if not self.validate_chain(_data['chain']):
                raise ValidatorError("@Validator - Bad job chain IDs.")
            else:
                _chain = []
                # Generate keywords for script substitutions
                _i = 0
                for _id in _data['chain']:
                    _variables["CIS_CHAIN%s" % _i] = _id
                    _i += 1
                    _chain.append(Jobs.JobChain(id=_id))
                logger.debug(
                    "@Validator - Job chain IDs passed validation: %s" %
                    _data['chain']
                )
                job.chain = _chain

        # Job scheduler
        if 'CIS_SCHEDULER' not in _variables.keys():
            raise ValidatorError(
                "@Validator - Scheduler not defined for job %s." % job.id())
        job.status.scheduler = _variables['CIS_SCHEDULER']

        # Update job data with default values
        job.data = Jobs.JobData(data=_variables)
        logger.log(VERBOSE, '@Validator - Validated input data:')
        logger.log(VERBOSE, _variables)

    def validate_value(self, key, value, service, nesting_level=0):
        """
        Validate value for specified service attribute.

        :param key: attribute name
        :param value: value to validate
        :param service: service object for which attribute is defined
        :return: True on success False otherwise.
        """

        # temporary cache variables
        variable_type = service.variables[key]['type']
        variable_allowed_values = service.variables[key]['values']

        if variable_type == 'string':
            # Attribute of type string check the table of allowed values
            if not value in variable_allowed_values:
                logger.warning("@Validator - Value not allowed: %s - %s." %
                               (key, value))
                return False
        elif variable_type == 'int':
            try:
                return self.validate_int(
                    key, value,
                    variable_allowed_values[0],
                    variable_allowed_values[1]
                )
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
        elif variable_type == 'float':
            try:
                return self.validate_float(
                    key, value,
                    variable_allowed_values[0],
                    variable_allowed_values[1]
                )
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
        elif variable_type == 'string_array':
            if not isinstance(value, list) and isinstance(value, tuple):
                logger.error(
                    "@Validator - Value is not a proper array:  %s", key
                )
                return False
            try:
                if len(value) > variable_allowed_values[0]:
                    logger.error(
                        "@Validator - Array  exceeds allowed length:  %s", key
                    )
                    return False
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            for _v in value:
                if not _v in variable_allowed_values[1:]:
                    logger.warning("@Validator - Value not allowed: %s - %s." %
                                   (key, value))
                    return False
        elif variable_type == 'int_array':
            if not isinstance(value, list) and isinstance(value, tuple):
                logger.error(
                    "@Validator - Value is not a proper array:  %s", key
                )
                return False
            try:
                if len(value) > variable_allowed_values[0]:
                    logger.error(
                        "@Validator - Array  exceeds allowed length:  %s", key
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
                _min = variable_allowed_values[1]
                _max = variable_allowed_values[2]
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            for _v in value:
                if not self.validate_int(key, _v, _min, _max):
                    return False
        elif variable_type == 'float_array':
            if not isinstance(value, list) and isinstance(value, tuple):
                logger.error(
                    "@Validator - Value is not a proper array:  %s", key
                )
                return False
            try:
                if len(value) > variable_allowed_values[0]:
                    logger.error(
                        "@Validator - Array  exceeds allowed length:  %s", key
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
                _min = variable_allowed_values[1]
                _max = variable_allowed_values[2]
            except IndexError:
                logger.error(
                    "@Validator - Badly defined range for variable:  %s" %
                    key
                )
                return False
            for _v in value:
                if not self.validate_float(key, _v, _min, _max):
                    return False
        elif variable_type == 'datetime':
            try:
                datetime.strptime(value, variable_allowed_values)
            except ValueError:
                logger.error(
                    "@Validator - DateTime variable given in unsupported format:  %s" %
                    key
                )
                return False
        elif variable_type == 'object':
            inner_variables = {
                _k: _v['default'] for _k, _v in variable_allowed_values.items()
            }
            for _k, _v in value.items():
                if _k in conf.service_reserved_keys or _k.startswith('CIS_CHAIN'):
                    raise ValidatorError(
                        "@Validator - '%s' variable name is restricted." % _k)
                elif _k in variable_allowed_values.keys():
                   inner_variables[_k] = _v
                else:
                    raise ValidatorError(
                        "@Validator - Not supported variable: %s." % _k)
                print inner_variables
            return False
        # for any unknown variable types when for example there is an error in service config
        # whole function should return False, but i keep it that way for now
        else:
             return False

        return True

    def validate_object(self):
        pass

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
        _finished = (_j.id() for _j in Jobs.StateManager.get_job_list('done'))
        for _id in chain:
            # ID of type string check if it is listed among finished jobs
            if not _id in _finished:
                logger.warning(
                    "@Validator - Job %s did not finish or does not exist. "
                    "Unable to chain output." %
                    _id)
                return False
        return True


class Service(dict):
    """
    Class implementing a Service.

    Stores Service configuration and monitors its state.

    """

    def __init__(self, name, data, *args, **kwargs):
        """Service C-tor

        :param name: The name of the service.
        :param data: Dict with service config read from JSON data file.

        Other arguments are passed to dict parent class.
        """
        # Service is a dict. Make all the keys accessible as attributes while
        # retaining the dict API
        super(Service, self).__init__(*args, **kwargs)
        self.__dict__ = self

        #: Service name
        self.name = name
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
        #: Definitions of validator plugins to use
        self.plugins = None  # @TODO disabled for now

        # Covert quota and job_size to bytes from MB
        self.config['quota'] = self.config['quota'] * 1000000
        self.config['job_size'] = self.config['job_size'] * 1000000


class ServiceStore(dict):
    def __init__(self):
        super(ServiceStore, self).__init__()

    def init(self):
        # Load all files from service_conf_path. Configuration files should be
        # in JSON format.
        logger.debug('Loading services configurations.')

        _path = conf.service_path_conf
        _services = os.listdir(_path)
        for _service in _services:
            # @TODO validate options structure
            _file_name = os.path.join(_path, _service)
            # Allow for subdirectories
            if not os.path.isfile(_file_name):
                continue
            with open(_file_name) as _f:
                _data = conf.json_load(_f)
            self[_service] = Service(_service, _data)

            logger.info("Initialized service: %s", _service)

        if logger.getEffectiveLevel() <= VERBOSE:
            logger.log(VERBOSE, json.dumps(self))


#: Validator singleton
Validator = Validator()
#: ServiceStore singleton
ServiceStore = ServiceStore()

