# -*- coding: UTF-8 -*-
"""
"""

import logging

from Config import conf, verbose

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
        #: Real space usage by service output files
        self.real_size = 0
        #: Projection of space usage by service output files
        self.current_size = 0
        # List of current job proxies. Job proxies are used to calculate
        # current service quota. Will be removed by the garbage collector to
        # indicate change in quota due to jobs scheduled for removal (but not
        # yet renmoved physically)
        self.__job_proxies = []
        self.__jobs = []  # List of current jobs with known size

        # Covert quota and job_size to bytes from MB
        self.config['quota'] = self.config['quota'] * 1000000
        self.config['job_size'] = self.config['job_size'] * 1000000

    def add_job_proxy(self, job):
        """
        Add new job proxy. The current service quota usage will increase by the
        amount defined in service config by job_size variable.

        :param job: :py:class:`Job` instance
        """
        if job.id in self.__job_proxies:
            logger.error("@Service - Job proxy already exists: %s" % job.id)
            return

        self.current_size += self.config['job_size']
        self.__job_proxies.append(job.id)
        verbose("@Service - Allocated %s MB for job proxy (%s)." %
                ((self.config['job_size']/1000000), self.name))

    def remove_job_proxy(self, job):
        """
        Remove a job proxy. The current service quota usage will decrease by
        the job size. Job size should have been determined by the update_job
        call.

        :param job: :py:class:`Job` instance
        """
        if job.id in self.__job_proxies:
            self.__job_proxies.remove(job.id)
            self.current_size -= job.get_size()

        verbose("@Service - Reclaimed %s MB of storage from soft "
                "quota (%s)." % ((job.get_size()/1000000), self.name))

    def update_job(self, job):
        """
        Calculate the current size of the output files of the job. Adjusts
        usage of the current and real quotas for the service.

        :param job: :py:class:`Job` instance
        """
        _change = 0
        if job.id in self.__jobs:
            _change -= job.get_size()
            self.current_size -= job.get_size()
            self.real_size -= job.get_size()
        else:
            self.__jobs.append(job.id)
            if job.id in self.__job_proxies:
                _change -= self.config['job_size']
                self.current_size -= self.config['job_size']
            else:
                self.__job_proxies.append(job.id)
        job.calculate_size()
        _change += job.get_size()
        self.current_size += job.get_size()
        self.real_size += job.get_size()
        verbose("@Service - Storage usage adjusted by %s MB (%s)." %
                ((_change/1000000), self.name))

    def remove_job(self, job):
        """
        Remove the job and its proxy. Adjust the usage of the service quota.

        :param job: :py:class:`Job` instance
        """
        if job.id not in self.__jobs:
            logger.error("@Service - Job does not exist: %s" % job.id)
            return
        if job.id in self.__job_proxies:
            self.__job_proxies.remove(job.id)
            self.current_size -= job.get_size()
        self.__jobs.remove(job.id)
        self.real_size -= job.get_size()
        verbose("@Service - Reclaimed %s MB of storage (%s)." %
                ((job.get_size()/1000000), self.name))


ServiceStore = {}


class JobStore(dict):
    def get_job_ids(self, state='all'):
        """
        Get IDs of existing jobs.

        :param state: State of the jobs to return. Valid values:

        * all
        * waiting
        * queued
        * running
        * closing
        * cleanup
        * done
        * failed
        * aborted
        * killed
        """

        _id_list = []
        for _id in self.keys():
            if state == 'all' or state == self[_id].get_state():
                _id_list.append(_id)

        return _id_list

JobStore = JobStore()
