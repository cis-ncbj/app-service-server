#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Global configuration for CISAppServer.
"""

import os
try:
    import json
except:
    import simplejson as json

from logging import \
    debug, log

VERBOSE = 5


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
        self.config_sleep_time = 1
        self.log_level = 'INFO'  #: Logging level
        self.log_output = None  #: Log output file name
        #: Path where PBS backend will store job IDs
        self.pbs_path_queue = 'PBS/Queue'
        #: Path where PBS backeng will create job working directories
        self.pbs_path_work = 'PBS/Scratch'
        self.pbs_default_queue = 'default'  #: Name of default PBS queue
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
            'removed'
        )
        #: Reserved key names for job parameters
        self.service_reserved_keys = ('service', 'name', 'scheduler', 'queue')
        #: Path to the shared storage used as communication medium with
        #: AppGateway
        self.gate_path_shared = 'Shared'
        #: Path where jobs output will be stored
        self.gate_path_output = 'Output'
        self.gate_path_jobs = None
        self.gate_path_exit = None
        self.gate_path_waiting = None
        self.gate_path_queued = None
        self.gate_path_running = None
        self.gate_path_done = None
        self.gate_path_failed = None
        self.gate_path_aborted = None
        self.gate_path_killed = None
        self.gate_path_removed = None
        self.gate_path = {
            "waiting": None,
            "queued": None,
            "running": None,
            "done": None,
            "failed": None,
            "aborted": None,
            "killed": None,
            "removed": None
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
            debug("@Config - Loading global configuration: %s" % conf_name)
            self.config_file = conf_name
            with open(self.config_file) as _conf_file:
                _conf = json.load(_conf_file)
            log(VERBOSE, json.dumps(_conf))
            self.update(_conf)

        debug('@Config - Finalise configuration initialisation')
        # Normalize paths to full versions
        for _key, _value in self.items():
            if '_path_' in _key and isinstance(_value, (str, unicode)):
                log(VERBOSE,'@Config - Correct path to full one: %s -> %s.' %
                      (_key, _value))
                self[_key] = os.path.realpath(_value)

        # Generate subdir names
        self.gate_path_jobs = os.path.join(self.gate_path_shared, 'jobs')
        self.gate_path_exit = os.path.join(self.gate_path_shared, 'exit')
        self.gate_path_waiting = os.path.join(self.gate_path_shared, 'waiting')
        self.gate_path_queued = os.path.join(self.gate_path_shared, 'queued')
        self.gate_path_running = os.path.join(self.gate_path_shared, 'running')
        self.gate_path_done = os.path.join(self.gate_path_shared, 'done')
        self.gate_path_failed = os.path.join(self.gate_path_shared, 'failed')
        self.gate_path_aborted = os.path.join(self.gate_path_shared, 'aborted')
        self.gate_path_killed = os.path.join(self.gate_path_shared, 'killed')
        self.gate_path_removed = os.path.join(self.gate_path_shared, 'removed')
        self.gate_path = {
            "waiting": self.gate_path_waiting,
            "queued": self.gate_path_queued,
            "running": self.gate_path_running,
            "done": self.gate_path_done,
            "failed": self.gate_path_failed,
            "aborted": self.gate_path_aborted,
            "killed": self.gate_path_killed,
            "removed": self.gate_path_removed
        }

        log(VERBOSE, self)


#: Global Config class instance. Use it to access the CISAppGateway
#: configuration.
conf = Config()
