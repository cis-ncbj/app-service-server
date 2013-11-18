#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Global configuration for CISAppServer.
"""

import os
import errno
import logging
import re
try:
    import json
except:
    import simplejson as json

logger = logging.getLogger(__name__)


VERBOSE = 5


def verbose(msg, exc_info=False):
    """
    Log message with VERBOSE log level.

    VERBOSE log level is higher than DEBUG and should be used for large debug
    messages, e.g. data dumps, output from subprocesses, etc.
    """
    logger.log(VERBOSE, msg, exc_info=exc_info)


class ExitCodes:
    """
    Job exit codes enum:

    * -100: Undefined (this should not happen)
    *  -99: Abort (default)
    *  -98: Shutdown
    *  -97: Delete
    *  -96: UserKill
    *  -95: Validate
    """
    Undefined, Abort, Shutdown, Delete, UserKill, Validate = range(-100, -94)


class Config(dict):
    """
    Class responsible for configuration storage and initialization.

    Config stores variables as instace members e.g.:

        conf.config_file

    The variables are also accessible via dictionary interface e.g.:

        conf['conf_file']
    """

    def __init__(self, *args, **kwargs):
        """
        Upon initialisation some default values are applied to the variables.
        However to finish the initialization :py:meth:`Config.load` method
        should be called.
        """
        # Config is a dict. Make all the keys accessible as attributes while
        # retaining the dict API
        super(Config, self).__init__(*args, **kwargs)
        self.__dict__ = self

        # Define default values
        self.config_file = None  #: Config file name
        #: Sleep interval in seconds between job status queries
        self.config_sleep_time = 3
        #: Every n-th status query dump the progress logs
        self.config_progress_step = 2
        #: Timeout for job cleanup before forcing shutdown
        self.config_shutdown_time = 2
        #: Enabled schedulers
        self.config_schedulers = ('pbs', 'ssh')
        #: Daemon mode pid file path
        self.daemon_path_pidfile = '/tmp/CISAppServer.pid'
        #: Timeout for daemon mode pid file acquisition
        self.daemon_pidfile_timeout = -1
        #: Working directory of daemon
        self.daemon_path_workdir = os.getcwd()
        #: AppServer install directory
        self.daemon_path_installdir = \
            os.path.dirname(os.path.realpath(__file__))
        self.log_level = 'INFO'  #: Logging level
        self.log_output = '/tmp/CISAppServer.log'  #: Log output file name
        self.log_level_cli = None  #: Logging level CLI override
        self.log_output_cli = None  #: Log output file name CLI override
        #: Configuration of logging module
        self.log_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'verbose': {
                    'format':
                    '%(levelname)s %(asctime)s %(module)s : %(message)s',
                    'datefmt': '%m-%d %H:%M:%S',
                },
                'simple': {
                    'format': '%(levelname)s %(message)s'
                },
            },
            'handlers': {
                'console': {
                    'level': self.log_level,
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                },
                'mail': {
                    'level': 'ERROR',
                    'class': 'logging.handlers.SMTPHandler',
                    'formatter': 'verbose',
                    'mailhost': 'localhost',
                    'fromaddr': 'kklimaszewski@cis.gov.pl',
                    'toaddrs': 'konrad.klimaszewski@gazeta.pl',
                    'subject': 'AppServer Error',
                },
                'file': {
                    'level': self.log_level,
                    'class': 'logging.handlers.RotatingFileHandler',
                    'formatter': 'verbose',
                    'filename': self.log_output,
                    'maxBytes': 10000000,
                    'backupCount': 5,
                }
            },
            'root': {
                'handlers': ['console', 'file', 'mail'],
                'level': self.log_level,
            }
        }
        #: Path where PBS backend will store job IDs
        self.pbs_path_queue = 'PBS/Queue'
        #: Path where PBS backeng will create job working directories
        self.pbs_path_work = 'PBS/Scratch'
        self.pbs_default_queue = 'short'  #: Name of default PBS queue
        self.pbs_max_jobs = 100  #: Maximum number of concurent PBS jobs
        #: Path where SSH backend will store job IDs
        self.ssh_path_queue = 'SSH/Queue'
        #: Path where SSH backeng will create job working directories
        self.ssh_path_work = 'SSH/Scratch'
        #: Name of default SSH execution host
        self.ssh_default_queue = 'localhost'
        #: Maximum number of concurent jobs per SSH execution host
        self.ssh_max_jobs = {
            'localhost': 2
        }
        #: Path with services configuration files
        self.service_path_conf = 'Services'
        #: Path with services scripts and input files
        self.service_path_data = 'Services/Data'
        #: Valid job states as well as names of directories on shared storage
        #: that are used to monitor job states
        self.service_states = (
            'waiting', 'queued', 'running', 'closing', 'cleanup',
            'done', 'failed', 'aborted', 'killed',
        )
        #: Allowed sections in job submission JSON
        self.service_allowed_sections = ('service', 'api', 'input', 'chain')
        #: Reserved key names for job parameters
        self.service_reserved_keys = (
            'CIS_SCHEDULER', 'CIS_QUEUE', 'CIS_SSH_HOST'
        )
        #: Default user name for job execution
        self.service_username = 'apprunner'
        #: Default job minimum lifetime in hours (supports fractions). Jobs
        #: that are younger then this cannot be removed by garbage collector
        self.service_min_lifetime = 2
        #: Default job maximum lifetime in hours (supports fractions). Jobs
        #: that are older then this will be removed by garbage collector.
        #: Setting this to zero means jobs can be immortal (at least until
        #: service quota is exceeded)
        self.service_max_lifetime = 24
        #: Default job maximum running time in hours (supports fractions). Jobs
        #: that are in running state longer then this will be killed and
        #: removed by garbage collector.
        self.service_max_runtime = 12
        #: Default maximum number of concurrent jobs allowed per dervice
        self.service_max_jobs = 80
        #: Defaul maximum disk size used by service output files in MB
        self.service_quota = 10000
        #: Default expected output size of a job in MB. It is used to estimate
        #: space requirements for jobs that are to be scheduled.
        self.service_job_size = 50
        #: Path to the shared storage used as communication medium with
        #: AppGateway
        self.gate_path_shared = 'Shared'
        #: Path where jobs output will be stored
        self.gate_path_output = 'Output'
        #: Path where jobs output is moved before removal (aleviates problems
        #: with files that are still in use)
        self.gate_path_dump = 'Dump'
        #: Path where jobs description is stored
        self.gate_path_jobs = None
        #: Path where jobs exit status is stored
        self.gate_path_exit = None
        #: Path where jobs internal state is stored
        self.gate_path_opts = None
        #: Path where waiting jobs are symlinked
        self.gate_path_waiting = None
        #: Path where queued jobs are symlinked
        self.gate_path_queued = None
        #: Path where running jobs are symlinked
        self.gate_path_running = None
        #: Path where jobs waiting for cleanup are symlinked
        self.gate_path_closing = None
        #: Path where jobs during cleanup are symlinked
        self.gate_path_cleanup = None
        #: Path were where done jobs are symlinked
        self.gate_path_done = None
        #: Path were where failed jobs are symlinked
        self.gate_path_failed = None
        #: Path were where aborted jobs are symlinked
        self.gate_path_aborted = None
        #: Path were where killed jobs are symlinked
        self.gate_path_killed = None
        #: Path were where jobs scheduled for removal are symlinked
        self.gate_path_delete = None
        #: Path were where jobs scheduled to be killed are symlinked
        self.gate_path_stop = None
        #: Dictionary of job states with corresponding paths
        self.gate_path = {
            "waiting": None,
            "queued": None,
            "running": None,
            "closing": None,
            "cleanup": None,
            "done": None,
            "failed": None,
            "aborted": None,
            "killed": None,
            "delete": None,
            "stop": None,
        }

    def load(self, conf_name=None):
        """
        Load CISAppServer configuration from JSON file and finalize the
        initialisation.

        :param conf_name: name of CISAppServer config file. When *None* is
            provided hardcoded defaults are used.
        """

        if conf_name is not None:
            # Load configuration from option file
            logger.debug("@Config - Loading global configuration: %s" %
                         conf_name)
            self.config_file = conf_name
            with open(self.config_file) as _conf_file:
                _conf = self.json_load(_conf_file)
            logger.log(VERBOSE, json.dumps(_conf))
            self.update(_conf)

        logger.debug('@Config - Finalise configuration initialisation')
        # Override config values by command line
        if self.log_level_cli is not None:
            self.log_level = self.log_level_cli
        if self.log_output_cli is not None:
            self.log_output = self.log_output_cli

        # Override logger settings
        if self.log_level is not None:
            for _key in self.log_config['handlers'].keys():
                if _key != 'mail':
                    self.log_config['handlers'][_key]['level'] = self.log_level
            self.log_config['root']['level'] = self.log_level
        if self.log_output is not None and \
           'file' in self.log_config['handlers'].keys():
            self.log_config['handlers']['file']['filename'] = \
                self.log_output

        # Normalize paths to full versions
        for _key, _value in self.items():
            if '_path_' in _key and isinstance(_value, (str, unicode)):
                logger.log(
                    VERBOSE,
                    '@Config - Correct path to full one: %s -> %s.' %
                    (_key, _value)
                )
                self[_key] = os.path.realpath(_value)

        # Generate subdir names
        self.gate_path_jobs = os.path.join(self.gate_path_shared, 'jobs')
        self.gate_path_exit = os.path.join(self.gate_path_shared, 'exit')
        self.gate_path_opts = os.path.join(self.gate_path_shared, 'opts')

        # Generate job state subdirs
        self.gate_path_waiting = os.path.join(self.gate_path_shared, 'waiting')
        self.gate_path_queued = os.path.join(self.gate_path_shared, 'queued')
        self.gate_path_running = os.path.join(self.gate_path_shared, 'running')
        self.gate_path_closing = os.path.join(self.gate_path_shared, 'closing')
        self.gate_path_cleanup = os.path.join(self.gate_path_shared, 'cleanup')
        self.gate_path_done = os.path.join(self.gate_path_shared, 'done')
        self.gate_path_failed = os.path.join(self.gate_path_shared, 'failed')
        self.gate_path_aborted = os.path.join(self.gate_path_shared, 'aborted')
        self.gate_path_killed = os.path.join(self.gate_path_shared, 'killed')
        self.gate_path_delete = os.path.join(self.gate_path_shared, 'delete')
        self.gate_path_stop = os.path.join(self.gate_path_shared, 'stop')
        self.gate_path = {
            "waiting": self.gate_path_waiting,
            "queued": self.gate_path_queued,
            "running": self.gate_path_running,
            "closing": self.gate_path_closing,
            "cleanup": self.gate_path_cleanup,
            "done": self.gate_path_done,
            "failed": self.gate_path_failed,
            "aborted": self.gate_path_aborted,
            "killed": self.gate_path_killed,
            "delete": self.gate_path_delete,
            "stop": self.gate_path_stop,
        }

        # Create those paths if they do not exist
        _mkdirs = [
            "daemon_path_workdir",
            "pbs_path_queue",
            "pbs_path_work",
            "ssh_path_queue",
            "ssh_path_work",
            "gate_path_shared",
            "gate_path_output",
            "gate_path_dump",
            "gate_path_jobs",
            "gate_path_exit",
            "gate_path_opts",
            "gate_path_delete",
            "gate_path_stop",
            "gate_path_waiting",
            "gate_path_queued",
            "gate_path_running",
            "gate_path_cleanup",
            "gate_path_closing",
            "gate_path_done",
            "gate_path_failed",
            "gate_path_aborted",
            "gate_path_killed",
        ]
        for _path in _mkdirs:
            if not os.path.isdir(self[_path]):
                self.mkdir_p(self[_path])

        logger.log(VERBOSE, self)

    def json_load(self, file):
        """
        Parse a JSON file

        First remove comments and then use the json module package
        Comments look like ::

                // ...
            or
                /*
                ...
                */

        Based on:
        http://www.lifl.fr/~riquetd/parse-a-json-file-with-comments.html.
        Much faster than https://github.com/getify/JSON.minify and
        https://gist.github.com/WizKid/1170297

        :param file: name of the file to parse.
        """
        content = ''.join(file.readlines())

        # Regular expression for comment
        comment_re = re.compile(
            '(^)?[^\S\n]*/(?:\*(.*?)\*/[^\S\n]*|/[^\n]*)($)?',
            re.DOTALL | re.MULTILINE
        )

        ## Looking for comments
        match = comment_re.search(content)
        while match:
            # single line comment
            content = content[:match.start()] + content[match.end():]
            match = comment_re.search(content)

        logger.log(VERBOSE, content)

        # Return json file
        return json.loads(content)

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise


#: Global Config class instance. Use it to access the CISAppGateway
#: configuration.
conf = Config()
