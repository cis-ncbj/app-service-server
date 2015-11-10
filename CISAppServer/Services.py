# -*- coding: UTF-8 -*-
"""
Module with implementations of the Service interface.
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

# Import full modules - resolves circular dependencies
import Jobs
import Globals as G
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
        self.api_max = 2.99
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
        for _service_name, _service in G.SERVICE_STORE.items():
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
        """

        # Do not validate jobs for the second time. This will conserve
        # resources in case scheduler queue is full and we try to resubmit
        if job.data:
            return

        # Make sure job.service is defined
        job.status.service = 'default'

        # Load job file from jobs directory
        _name = os.path.join(conf.gate_path_jobs, job.id())
        with open(_name) as _f:
            _data = json.load(_f)
        logger.debug(u'@Job - Loaded data file %s.', job.id())
        logger.log(VERBOSE, _data)

        # Check if data contains service attribute and that such service was
        # initialized
        if 'service' not in _data or \
                _data['service'] not in G.SERVICE_STORE or \
                _data['service'] == 'default':
            raise ValidatorError("Not supported service: %s." %
                                 _data['service'])

        job.status.service = _data['service']
        _service = G.SERVICE_STORE[_data['service']]
        job.status.scheduler = _service.config['scheduler']

        # Make sure that input dictionary exists
        if 'input' not in _data:
            _data['input'] = {}
        elif not isinstance(_data['input'], dict):
            raise ValidatorError("The 'input' section is not a dictionary")

        # Make sure API level is correct
        if 'api' not in _data:
            raise ValidatorError("Job did not specify API level.")
        if not self.validate_float(['api'], _data['api'],
                                   self.api_min, self.api_max):
            raise ValidatorError("API level %s is not supported." %
                                 _data['api'])
        elif float(_data['api']) < self.api_current:
            # Deprecated API requested. Mark as such
            job.set_flag(Jobs.JobState.FLAG_OLD_API)

        # Make sure no unsupported sections were passed
        for _k in _data:
            if _k not in conf.service_allowed_sections:
                raise ValidatorError("Section '%s' is not allowed in job "
                                     "definition." % _k)

        # Load defaults
        _variables = {
            _k: _v['default'] for _k, _v in
            _service.variables.items()
        }

        # Load sets
        for _k, _v in _data['input'].items():
            if _k in _service.sets:
                if isinstance(_v, str) or isinstance(_v, unicode):
                    # Value specified as string - check the format using python
                    # builtin conversion
                    _v = int(_v)
                elif not isinstance(_v, int):
                    # Value specified neither as int nor string - raise error
                    raise ValidatorError(
                        "Set variables have to be of type int or "
                        "string. (%s: %s)" % (_k, _v)
                    )
                if _v != 1:
                    raise ValidatorError(
                        "Set variables only accept value of 1. "
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
                    "The '%s' variable name is restricted." % _k)
            elif _k in _service.variables:
                _variables[_k] = _v
            else:
                raise ValidatorError(
                    "Not supported variable: %s." % _k)

        # Check that all attribute names are defined in service configuration
        # Validate values of the attributes
        for _k, _v in _variables.items():
            if _k in _service.variables:
                _variables[_k] = self.validate_value(
                    [_k], _v, _service.variables[_k])
                logger.debug(
                    "Value passed validation: %s = %s", _k, _v
                )
            # Check for possible reserved attribute names like service, name,
            # date
            elif _k in conf.service_reserved_keys:
                _variables[_k] = self.validate_value([_k], _v,
                                                     G.SERVICE_STORE['default'].variables[_k])
                logger.debug(
                    "Value passed validation: %s = %s", _k, _v
                )
            else:
                raise ValidatorError("Not supported variable: %s." % _k)

        # Validate job output chaining. Check if defined job IDs point to
        # existing jobs in 'done' state.
        if 'chain' in _data:
            if not isinstance(_data['chain'], list) and \
                    not isinstance(_data['chain'], tuple):
                raise ValidatorError("The 'chain' section is not a list")

            self.validate_chain(_data['chain'])

            _chain = []
            # Generate keywords for script substitutions
            _i = 0
            for _id in _data['chain']:
                _variables["CIS_CHAIN%s" % _i] = _id
                _i += 1
                _chain.append(Jobs.JobChain(id=_id))
            logger.debug(
                "Job chain IDs passed validation: %s" %
                _data['chain']
            )
            job.chain = _chain

        # Job scheduler
        if 'CIS_SCHEDULER' in _variables:
            job.status.scheduler = _variables['CIS_SCHEDULER']

        # Update job data with default values
        job.data = Jobs.JobData(data=_variables)
        logger.log(VERBOSE, 'Validated input data:')
        logger.log(VERBOSE, _variables)

    def validate_value(self, path, value, template, nesting_level=0):
        """
        Validate value for specified service attribute.

        :param path: a list of nested attribute names for logging, e.g. [object, list_attribute, 10]
        :param value: value to validate
        :param template: dictionary describing the variable
        :param nesting_level: current nesting level
        :return: validated variable

        The parameter **template** should be of the following form::

            {
                'type': 'float_array',
                'default':[1.0, 2.5],
                'values':[0,100],
                'length':10
            }

        - Allowed 'type's:
            * string
            * int
            * float
            * datetime
            * object
            * string_array
            * int_array
            * float_array
            * datetime_array
            * object_array

        - 'default' should be of an appropriate type.

        - 'values' defines allowed value of the variable:
            * string - white list of strings
            * int, float - [min. max]
            * datetime - strptime format string
            * object - dictionary with keys being attribute names and values dictionaries defining variable templates

        - 'length' is only mandatory for array types and defines maximum allowed length
        """

        # temporary cache variables
        _variable_type = template['type']
        _variable_allowed_values = template['values']

        # Run specific validate method based on type of the validated value
        if _variable_type == 'string':
            # Attribute of type string check the table of allowed values
            if value not in _variable_allowed_values:
                raise ValidatorError(
                    "%s = %s - Value not in the white list (%s)." %
                    (".".join(path), value, _variable_allowed_values))
            return value
        elif _variable_type == 'int':
            try:
                return self.validate_int(
                    path, value,
                    _variable_allowed_values[0],
                    _variable_allowed_values[1]
                )
            except IndexError:
                raise ValidatorError(
                    "Wrong range definition for variable:  %s" % ".".join(path)
                )
        elif _variable_type == 'float':
            try:
                return self.validate_float(
                    path, value,
                    _variable_allowed_values[0],
                    _variable_allowed_values[1]
                )
            except IndexError:
                raise ValidatorError(
                    "Wrong range definition for variable:  %s" % ".".join(path)
                )
        elif _variable_type == 'datetime':
            try:
                datetime.strptime(value, _variable_allowed_values)
            except ValueError:
                raise ValidatorError(
                    "%s = %s - value not in supported format (%s)" %
                    (".".join(path), value, _variable_allowed_values)
                )
            except TypeError:
                raise ValidatorError(
                    "%s = %s - date should be provided as a string (%s)" %
                    (".".join(path), value, _variable_allowed_values)
                )
            return value
        elif _variable_type == 'object':
            # prevent from infinite recurrence
            if nesting_level >= conf.service_max_nesting_level:
                raise ValidatorError(
                    "Unsupported object nesting level above %d :  %s" %
                    (conf.service_max_nesting_level, ".".join(path))
                )
            return self.validate_object(
                path, value, _variable_allowed_values, nesting_level)
        elif _variable_type == 'string_array':
            return self.validate_array(
                path, value, template["length"],
                {'type': 'string', 'values': _variable_allowed_values},
                nesting_level
            )
        elif _variable_type == 'int_array':
            return self.validate_array(
                path, value, template["length"],
                {'type': 'int', 'values': _variable_allowed_values},
                nesting_level
            )
        elif _variable_type == 'float_array':
            return self.validate_array(
                path, value, template["length"],
                {'type': 'float', 'values': _variable_allowed_values},
                nesting_level
            )
        elif _variable_type == 'object_array':
            return self.validate_array(
                path, value, template["length"],
                {'type': 'object', 'values': _variable_allowed_values},
                nesting_level
            )

        raise ValidatorError("(%s)%s - Unknown variable type" %
                             (_variable_type, ".".join(path)))

    def validate_array(self, path, value, length, template, nesting_level=0):
        """
        Validate array types: string_array, int_array, float_array, datetime_array, object_array
        :param path: a list of nested attribute names for logging, e.g. [object, list_attribute, 10]
        :param value: value to validate
        :param length: maximum allowed length of the array
        :param template: template defining elements of the array
        :param nesting_level: current nesting level
        :return: validated array
        """
        if not isinstance(value, list) and not isinstance(value, tuple):
            raise ValidatorError(
                "%s is not a proper array" % ".".join(path)
            )
        try:
            if len(value) > length:
                raise ValidatorError(
                    "len(%s) = %s - array exceeds allowed length (%s)" %
                    (".".join(path), len(value), length)
                )
        except IndexError:
            raise ValidatorError(
                "%s has wrong range definition" % ".".join(path)
            )
        _i = 0
        _result = []
        for _v in value:
            _result.append(self.validate_value(
                path + [str(_i)],
                _v,
                {
                    "type": template["type"],
                    "values": template["values"]
                },
                nesting_level
            ))
        return _result

    def validate_object(self, path, value, template, nesting_level):
        """
        Validate object type
        :param path: a list of nested attribute names for logging, e.g. [object, list_attribute, 10]
        :param value: value to validate
        :param template: template defining the object structure
        :param nesting_level: current nesting level
        :return: validated array
        """
        # Check the value format
        if not isinstance(value, dict):
            raise ValidatorError(
                "Value is not a proper dictionary:  %s" % ".".join(path)
            )
        # Increase recurrence level
        nesting_level += 1

        # Load defaults
        _inner_variables = {
            _k: _v['default'] for _k, _v in template.items()
        }
        for _k, _v in value.items():
            # Reserved keys
            if _k in conf.service_reserved_keys or _k.startswith('CIS_CHAIN'):
                raise ValidatorError(
                    "The attribute '%s' name of object '%s' is restricted." %
                    (_k, ".".join(path)))
            elif _k in template:
                # Validate value using reccurence
                _inner_variables[_k] = self.validate_value(
                    path + [_k],
                    _v,
                    template[_k],
                    nesting_level)
            else:
                raise ValidatorError(
                    "Not supported attribute '%s' for object '%s'" %
                    (_k, ".".join(path)))
        return _inner_variables

    def validate_int(self, path, value, min, max):
        """
        Validate integer type
        :param path: a list of nested attribute names for logging, e.g. [object, list_attribute, 10]
        :param value: value to validate
        :param min: minimal allowed value
        :param max: maximum allowed value
        :return: validated int
        """
        # Attribute of type int - check the format
        if isinstance(value, str) or isinstance(value, unicode):
            # Value specified as string - check the format using python
            # builtin conversion
            try:
                _v = int(value)
            except ValueError:
                raise ValidatorError("%s = %s - value does not decribe an integer" %
                                     (".".join(path), value)
                )
        elif not isinstance(value, int):
            # Value specified neither as int nor string - raise error
            raise ValidatorError("%s = %s - value is not an int" %
                                 (".".join(path), value))
        else:
            _v = value

        # Check that atrribute value falls in allowed range
        if _v < min or _v > max:
            raise ValidatorError(
                "%s = %s - value not in allowed range (%s)" %
                (".".join(path), _v, (min, max))
            )

        return _v

    def validate_float(self, path, value, min, max):
        """
        Validate float type
        :param path: a list of nested attribute names for logging, e.g. [object, list_attribute, 10]
        :param value: value to validate
        :param min: minimal allowed value
        :param max: maximum allowed value
        :return: validated float
        """
        # Attribute of type float - check the format
        if isinstance(value, str) or isinstance(value, unicode):
            # Value specified as string - check the format using python
            # builtin conversion
            try:
                _v = float(value)
            except ValueError:
                raise ValidatorError("%s = %s - value does not decribe a float" %
                                     (".".join(path), value)
                )
        elif not isinstance(value, float) and not isinstance(value, int):
            # Value specified neither as float nor string - raise error
            raise ValidatorError("%s = %s - value is not a float" %
                                 (".".join(path), value))
        else:
            _v = value

        # Check that atrribute value falls in allowed range
        if _v < min or _v > max:
            raise ValidatorError(
                "%s = %s - value not in allowed range (%s)" %
                (".".join(path), _v, (min, max))
            )

        return _v

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

    def validate_chain(self, chain):
        """
        Validate input chains
        :param chain: list of job IDs this job depends on.
        """
        # TODO check only if the chain exists not if its done. Make this job
        # wait if it is not finished
        _finished = []
        try:
            _session = G.STATE_MANAGER.new_session()
            _finished = list(
                    _j.id() for _j in G.STATE_MANAGER.get_job_list(
                        'done', session=_session
                    )
                )
            _session.close()
        except:
            logger.error("@PBS - Unable to connect to DB.", exc_info=True)
        logger.debug('Finished jobs: %s', _finished)
        for _id in chain:
            # ID of type string check if it is listed among finished jobs
            if _id not in _finished:
                raise ValidatorError(
                        "Chain job %s did not finish or does not exist."
                        % _id)


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
            'username': conf.service_username,
            'scheduler': conf.service_default_scheduler,
            'queue': G.SCHEDULER_STORE[conf.service_default_scheduler].default_queue
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
            # Check if name of the service was specified. Use filename otherwise
            if "name" in _data:
                _service = _data["name"]
            self[_service] = Service(_service, _data)

            logger.info("Initialized service: %s", _service)

        if logger.getEffectiveLevel() <= VERBOSE:
            logger.log(VERBOSE, json.dumps(self))

