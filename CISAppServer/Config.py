#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Global configuration for CISAppServer.
"""

import os
import logging
import re
try:
    import json
except:
    import simplejson as json

VERBOSE = 5


logger = logging.getLogger(__name__)


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
        #: Sleep interval between job status queries
        self.config_sleep_time = 3
        #: Every n-th status query dump the progress logs
        self.config_progress_step = 2
        #: Interval in hours after which job is automatically removed with all
        #: data
        self.config_delete_interval = 48
        #: Interval in hours after which job is automatically killed if in
        #: running state
        self.config_kill_interval = 24
        #: Daemon mode pid file path
        self.daemon_path_pidfile = '/tmp/CISAppServer.pid'
        #: Timeout for daemon mode pid file acquisition
        self.daemon_pidfile_timeout = -1
        #: Working directory of daemon
        self.daemon_path_workdir = os.getcwd()
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
        #: Path with services configuration files
        self.service_path_conf = 'Services'
        #: Path with services scripts and input files
        self.service_path_data = 'Services/Data'
        #: Valid job states as well as names of directories on shared storage
        #: that are used to monitor job states
        self.service_states = (
            'waiting', 'queued', 'running',
            'done', 'failed', 'aborted', 'killed',
            'delete'
        )
        #: Reserved key names for job parameters
        self.service_reserved_keys = ('service', 'name', 'scheduler', 'queue')
        #: Default job minimum lifetime in minutes. Jobs that are younger then
        #: this cannot be removed by garbage collector
        self.service_min_lifetime = 120
        #: Default job maximum lifetime in minutes. Jobs that are older then
        #: this will be removed by garbage collector. Setting this to zero
        #: means jobs can be immortal (at least until service quota is
        #: exceeded)
        self.service_max_lifetime = 2880
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
        #: Path were jobs description is stored
        self.gate_path_jobs = None
        #: Path were jobs exit status is stored
        self.gate_path_exit = None
        #: Path were where waiting jobs are symlinked
        self.gate_path_waiting = None
        #: Path were where queued jobs are symlinked
        self.gate_path_queued = None
        #: Path were where running jobs are symlinked
        self.gate_path_running = None
        #: Path were where done jobs are symlinked
        self.gate_path_done = None
        #: Path were where failed jobs are symlinked
        self.gate_path_failed = None
        #: Path were where aborted jobs are symlinked
        self.gate_path_aborted = None
        #: Path were where killed jobs are symlinked
        self.gate_path_killed = None
        #: Path were where jobs shceduled for removal are symlinked
        self.gate_path_delete = None
        #: Dictionary of job states with corresponding paths
        self.gate_path = {
            "waiting": None,
            "queued": None,
            "running": None,
            "done": None,
            "failed": None,
            "aborted": None,
            "killed": None,
            "delete": None,
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

        # Generate job state subdirs
        self.gate_path_waiting = os.path.join(self.gate_path_shared, 'waiting')
        self.gate_path_queued = os.path.join(self.gate_path_shared, 'queued')
        self.gate_path_running = os.path.join(self.gate_path_shared, 'running')
        self.gate_path_done = os.path.join(self.gate_path_shared, 'done')
        self.gate_path_failed = os.path.join(self.gate_path_shared, 'failed')
        self.gate_path_aborted = os.path.join(self.gate_path_shared, 'aborted')
        self.gate_path_killed = os.path.join(self.gate_path_shared, 'killed')
        self.gate_path_delete = os.path.join(self.gate_path_shared, 'delete')
        self.gate_path = {
            "waiting": self.gate_path_waiting,
            "queued": self.gate_path_queued,
            "running": self.gate_path_running,
            "done": self.gate_path_done,
            "failed": self.gate_path_failed,
            "aborted": self.gate_path_aborted,
            "killed": self.gate_path_killed,
            "delete": self.gate_path_delete,
        }

        # Create those paths if they do not exist
        _mkdirs = [
            "daemon_path_workdir",
            "pbs_path_queue",
            "pbs_path_work",
            "gate_path_shared",
            "gate_path_output",
            "gate_path_dump",
            "gate_path_jobs",
            "gate_path_exit",
            "gate_path_delete",
            "gate_path_waiting",
            "gate_path_queued",
            "gate_path_running",
            "gate_path_done",
            "gate_path_failed",
            "gate_path_aborted",
            "gate_path_killed",
        ]
        for _path in _mkdirs:
            if not os.path.isdir(self[_path]):
                os.mkdir(self[_path])

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


#: Global Config class instance. Use it to access the CISAppGateway
#: configuration.
conf = Config()
