# -*- coding: UTF-8 -*-
"""
"""

import logging

from collections import OrderedDict

from Config import conf, VERBOSE

logger = logging.getLogger(__name__)


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

        # Covert quota and job_size to bytes from MB
        self.config['quota'] = self.config['quota'] * 1000000
        self.config['job_size'] = self.config['job_size'] * 1000000


ServiceStore = {}

SchedulerStore = {}
