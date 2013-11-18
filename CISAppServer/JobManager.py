#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Main module of CISAppServer. Responsible for job management.
"""

import os
import shutil
import time
import logging
import threading
from datetime import datetime, timedelta

import Tools as T

from Config import conf, verbose, ExitCodes
from Jobs import Job
from DataStore import ServiceStore, JobStore

version = "0.3"

logger = logging.getLogger(__name__)


class JobManager(object):
    """
    Main calss of CISAppServer. It is responsible for job management.
    """

    def __init__(self):
        """
        Upon initialization stes up Validator and Sheduler interfaces.
        """
        self.init()

    def init(self):
        """
        Initialize JobManager. Creates new instaces of Validator and
        Schedulers. Loads existing jobs from file system.

        Existing state is purged.
        """
        # Initialize Validator and PbsManager
        self.validator = T.Validator()  #: Validator instance
        self.schedulers = {}  #: Scheduler interface instances
        for _scheduler in conf.config_schedulers:
            if _scheduler == 'pbs':
                self.schedulers[_scheduler] = T.PbsScheduler()
            elif _scheduler == 'ssh':
                self.schedulers[_scheduler] = T.SshScheduler()

        # State of the job queue
        self.__queue_running = True
        # Size of service output data last time quota was exceeded
        self.__last_service_size = {}
        # Warning counter - quota
        self.__w_counter_quota = {}
        for _s in ServiceStore.keys():
            self.__w_counter_quota[_s] = 0
        # Thread list
        self.__thread_list = []

        # Load existing jobs
        _list = os.listdir(conf.gate_path_jobs)
        for _jid in _list:
            logger.debug('@JManager - Detected active job %s.' % _jid)

            # Create new job instance
            _job = Job(_jid)
            if _job is None:
                continue

            if _jid in JobStore.keys():
                _job.die("@JManager - Job ID already used: %s" % _jid)

            JobStore[_jid] = _job
            # Make sure Job.service and Job.valid_data is present
            self.validator.validate(_job)
            # Update service quota
            if os.path.isdir(os.path.join(conf.gate_path_output, _jid)):
                ServiceStore[_job.service].update_job(_job)
            # Reduce memory footprint
            if _job.get_state() in ('done', 'failed', 'killed', 'aborted'):
                _job.compact()

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
        for _id in JobStore.keys():
            if state == 'all' or state == JobStore[_id].get_state():
                _id_list.append(_id)

        return _id_list

    def get_job(self, job_id, create=False):
        """
        Get Job object from internal list.

        :param job_id: Job unique ID,
        :param create: If job identified by *job_id* is not found in the list
            an ERROR message is logged. When create is True a new Job object is
            created and added to the list,
        :return: Job object if *job_id* was found, None otherwise.
        """

        # Handle zombies
        if job_id not in JobStore.keys():
            if create:
                _job = Job(job_id)
                JobStore[job_id] = _job
                return _job
            else:
                logger.error('@JManager - Job %s is missing from '
                             'overall job list.' % job_id)
                return None

        return JobStore[job_id]

    def check_new_jobs(self):
        """
        Check for new job files in the queue directory.

        If found try to submit them to selected job scheduler.
        """

        # New jobs are put into the "waiting" directory
        try:
            _queue = os.listdir(conf.gate_path_waiting)
        except:
            logger.error("@JManager - Unable to read waiting queue %s" %
                         conf.gate_path_waiting, exc_info=True)
            return

        for _jid in _queue:
            logger.debug('@JManager - Detected new job %s.' % _jid)

            # Create new job instance. It is possible that it is already
            # created during initialization or while checking for old jobs to
            # remove, therefore use self.get_job.
            _job = self.get_job(_jid, create=True)
            if _job is None:
                continue

            if not self.validator.validate(_job):  # Validate input
                continue

            _service_name = _job.service
            if not self.collect_garbage(_service_name):
                if self.__w_counter_quota[_service_name] < \
                   6 * 60 * 60 / conf.config_sleep_time:
                    self.__w_counter_quota[_service_name] += 1
                    if ServiceStore[_service_name].current_size != \
                       self.__last_service_size[_service_name]:
                        logger.warning(
                            "@JManager - Cannot collect garbage for service: "
                            "%s" % _job.service
                        )
                        self.__last_service_size[_service_name] = \
                            ServiceStore[_service_name].current_size
                else:
                    logger.error(
                        "@JManager - Cannot collect garbage for service: %s. "
                        "Message repeated 100 times." %
                        _job.service
                    )
                    self.__w_counter_quota[_service_name] = 0
                continue
            else:
                self.__last_service_size[_service_name] = \
                    ServiceStore[_service_name].current_size
                self.__w_counter_quota[_service_name] = 0

            try:
                if self.submit(_job):
                    _job.queue()
                    ServiceStore[_service_name].add_job_proxy(_job)
                # Submit can return False when queue is full. Do not terminate
                # job here so it can be resubmitted next time. If submission
                # failed scheduler should have set job state to Aborted anyway.
            except:
                _job.die("@JManager - Cannot start job %s." % _jid,
                         exc_info=True)

    def check_running_jobs(self):
        """
        Check status of running jobs.

        Finished jobs will be marked for finalisation
        """

        # Loop over supported schedulers
        for _sname, _scheduler in self.schedulers.items():
            # check the "queue_path" for files with scheduler ids. this
            # directory should only be accessible inside of the firewall so it
            # should be safe from corruption by clients.
            try:
                _queue = os.listdir(_scheduler.queue_path)
            except:
                logger.error(
                    "@JManager - unable to read %s queue directory %s." %
                    (_sname, _scheduler.queue_path),
                    exc_info=True
                )
                continue

            _jobs = []  # Active job list
            for _jid in _queue:
                # Get Job object
                _job = self.get_job(_jid)
                # Handle zombies
                if _job is None:
                    logger.error(
                        '@JManager - Job %s in %s queue is missing from '
                        'overall job list.' % (_jid, _sname)
                    )
                    try:
                        os.unlink(os.path.join(_scheduler.queue_path, _jid))
                    except:
                        logger.error(
                            '@JManager - Unable to remove dangling job ID '
                            '%s from %s queue.' % (_jid, _sname)
                        )
                    continue

                # Scheduler can change state for only running and waiting jobs.
                # Disregard the rest.
                _state = _job.get_state()
                if _state == 'closing' or _state == 'cleanup':
                    continue
                elif _state == 'running' or _state == 'queued':
                    _jobs.append(_job)
                else:
                    _job.die("@JManager - job state %s not allowed while in "
                             "scheduler queue" % _state)

            # Ask the scheduler to run the update
            _scheduler.update(_jobs)

    def check_cleanup(self):
        """
        Check jobs marked for cleanup.

        Starts the cleanup for the jobs in separate threads.
        """
        # Jobs ready for cleanup are put into the "closing" directory
        try:
            _queue = os.listdir(conf.gate_path_closing)
        except:
            logger.error("@JManager - Unable to read cleanup queue %s" %
                         conf.gate_path_closing, exc_info=True)
            return

        for _jid in _queue:
            logger.debug('@JManager - Detected cleanup job %s.' % _jid)

            # Create new job instance. It is possible that it is already
            # created during initialization or while checking for old jobs to
            # remove, therefore use self.get_job.
            _job = self.get_job(_jid, create=True)
            if _job is None:
                continue

            # Check for valid input data
            if not _job.get_exit_state():
                _job.die("@JManager - Job %s in closing state yet no exit "
                         "state defined." % _jid)
                continue
            if not _job.valid_data:
                if _job.get_exit_state() == 'aborted':
                    _job.exit()
                else:
                    _job.die("@JManager - Job %s has \"%s\" exit state set yet"
                             "there is no validated input data." %
                             (_jid, _job.get_exit_state()))
                continue

            _scheduler = self.schedulers[_job.valid_data['CIS_SCHEDULER']]
            # Run cleanup in separate threads
            try:
                # Mark job as in cleanup state
                _job.cleanup()
                if _job.get_exit_state() == 'aborted':
                    _thread = threading.Thread(
                        target=_scheduler.abort, args=(_job,))
                    _thread.start()
                    self.__thread_list.append(_thread)
                    logger.debug("@JManager - Abort cleanup thread started "
                                 "for job %s" % _job.id)
                else:
                    _thread = threading.Thread(
                        target=_scheduler.finalise, args=(_job,))
                    _thread.start()
                    self.__thread_list.append(_thread)
                    logger.debug("@JManager - Finalise cleanup thread started "
                                 "for job %s" % _job.id)
            except:
                logger.error("@JManager - Unable to start cleanup thread "
                             "for job %s" % _job.id, exc_info=True)

    def check_job_kill_requests(self):
        """
        Check for job kill requests.

        If found and job is still running kill it.
        """

        # Symlinks in "stop" dir mark jobs for removal
        try:
            _queue = os.listdir(conf.gate_path_stop)
        except:
            logger.error("@JManager - Unable to read kill queue: %s." %
                         conf.gate_path_stop, exc_info=True)
            return

        for _jid in _queue:
            logger.debug('@JManager - Detected job marked for a kill: %s' %
                         _jid)

            _job = self.get_job(_jid, create=True)
            if _job is None:
                continue

            # Stop if it is running
            if _job.get_state() == 'running' or \
                    _job.get_state() == 'queued':
                self.schedulers[_job.valid_data['CIS_SCHEDULER']].stop(
                    _job, 'User request', ExitCodes.UserKill
                )
            elif _job.get_state() == 'waiting':
                _job.finish('User request', 'killed', ExitCodes.UserKill)
            else:
                logger.warning("@JManager - Cannot kill job %s. "
                               "It is already finished." % _jid)

            # Remove the kill mark
            try:
                os.unlink(os.path.join(conf.gate_path_stop, _jid))
            except:
                logger.error("Cannot remove kill mark for job %s." % _jid,
                             exc_info=True)

    def check_deleted_jobs(self):
        """
        Check for jobs marked for removal.

        If found remove all resources related to the job. If a job is still
        running kill it.
        """

        # Symlinks in "delete" dir mark jobs for removal
        try:
            _queue = os.listdir(conf.gate_path_delete)
        except:
            logger.error("@JManager - Unable to read delete queue: %s." %
                         conf.gate_path_delete, exc_info=True)
            return

        for _jid in _queue:
            logger.debug('@JManager - Detected job marked for deletion: %s' %
                         _jid)

            _job = self.get_job(_jid, create=True)
            if _job is None:
                continue

            # Stop if it is running
            if _job.get_state() in ('running', 'queued'):
                self.schedulers[_job.valid_data['CIS_SCHEDULER']].stop(
                    _job, "User request", ExitCodes.Delete
                )
                continue

            if _job.get_state() in ('cleanup', 'running'):
                continue

            try:
                # Remove job symlinks
                for _state, _path in conf.gate_path.items():
                    _name = os.path.join(_path, _jid)
                    if os.path.exists(_name):
                        os.unlink(_name)
                # Remove job file after symlinks (otherwise os.path.exists
                # fails on symlinks)
                os.unlink(os.path.join(conf.gate_path_jobs, _jid))
                # Remove output status file
                _name = os.path.join(conf.gate_path_exit, _jid)
                if os.path.exists(_name):
                    os.unlink(_name)
            except:
                logger.error("Cannot remove job %s." % _jid, exc_info=True)

            # Remove the output directory and its contents
            _output = os.path.join(conf.gate_path_output, _jid)
            _dump = os.path.join(conf.gate_path_dump, _jid)
            try:
                if os.path.isdir(_output):
                    shutil.move(_output, _dump)
                    shutil.rmtree(_dump, onerror=T.rmtree_error)
                    # Update service quota status
                    ServiceStore[_job.service].remove_job(_job)
            except:
                logger.error("Cannot remove job output %s." % _jid,
                             exc_info=True)

            # Remove job from the list
            del JobStore[_jid]
            logger.info('@JManager - Job %s removed with all data.' %
                        _jid)

    def check_old_jobs(self):
        """Check for jobs that exceed their life time.

        If found mark them for removal."""

        try:
            _queue = os.listdir(conf.gate_path_jobs)
        except:
            logger.error(u"@JManager - Unable to read job queue: %s." %
                         conf.gate_path_jobs, exc_info=True)
            return

        for _jid in _queue:
            _job = self.get_job(_jid, create=True)
            if _job is None or _job.service is None:
                continue

            _delete_dt = ServiceStore[_job.service].config['max_lifetime']
            if _delete_dt == 0:
                continue
            _state = _job.get_state()
            _now = datetime.now()
            _dt = timedelta(hours=_delete_dt)
            _path = None
            try:
                if _state in ['killed', 'aborted']:
                    _path = os.path.join(conf.gate_path_output, _jid)
                    # If job was killed in Waiting or Queued states it could
                    # have not produced output
                    if not os.path.isdir(_path):
                        _path = os.path.join(conf.gate_path_jobs, _jid)
                elif _state in ['done', 'failed']:
                    _path = os.path.join(conf.gate_path_output, _jid)
                elif _state == 'running':
                    _path = os.path.join(conf.gate_path_running, _jid)
                    _delete_dt = ServiceStore[
                        _job.service].config['max_runtime']
                    _dt = timedelta(hours=_delete_dt)
                else:
                    continue

                _path_time = datetime.fromtimestamp(os.path.getctime(_path))
                verbose("@JManager - Removal dt: %s" % _dt)
                verbose("@JManager - Path time: %s" % _path_time)
                verbose("@JManager - Current time: %s" % _now)
                verbose("@JManager - Time diff: %s" %
                        (_path_time + _dt - _now))
                _path_time += _dt
            except:
                logger.error(
                    "@JManager - Unable to extract job change time: %s." %
                    _jid, exc_info=True)
                continue

            if _path_time < _now:
                logger.info("@JManager - Job reached storage time limit. "
                            "Sheduling for removal.")
                _job.delete()

        # Remove finished threads
        for _thread in self.__thread_list:
            if not _thread.is_alive():
                _thread.join()
                self.__thread_list.remove(_thread)
                logger.debug("@JManager - Removed finished cleanup thread.")

    def collect_garbage(self, service, full=False):
        """
        Check if service quota is not exceeded. If yes remove oldest finished
        jobs.

        :param service: Service name for which garbage collection should be
            performed,
        :param delta: Perform the quota check as if current disk usage was
            increased by delta MBs.
        :param full: If True force garbage collection even if disk usage is
            not above alloted quota. In addition removes all jobs older than
            min job life time.
        :return: True if quota is not reached or garbage collection succeeded.
        """
        # Get Service instance
        _service = ServiceStore[service]
        _start_size = _service.current_size
        _delta = _service.config['job_size']
        _quota = _service.config['quota']

        # Check quota - size is stored in bytes while quota and delta in MBs
        if _service.current_size + _delta < _quota and \
           _service.real_size < _quota * 1.3 and \
           not full:
            return True

        logger.debug("@JManager - Quota for service %s exceeded." % service)
        verbose("Quota: %s" % _quota)
        verbose("Delta: %s" % _delta)
        verbose("Size: %s" % _service.current_size)

        _job_table = []  # List of tuples (lifetime, job)
        for _jid, _job in JobStore.items():
            if _job.service != service:
                continue
            # Get protection interval
            _protect_dt = _service.config['min_lifetime']
            _dt = timedelta(hours=_protect_dt)
            _state = _job.get_state()
            _now = datetime.now()
            _path = None
            try:
                # We consider only jobs that could have produced output
                if _state in ['done', 'failed', 'killed', 'aborted']:
                    _path = os.path.join(conf.gate_path_output, _jid)
                    if not os.path.isdir(_path):
                        continue
                else:
                    continue

                # Jobs that are too young and are still in protection interval
                # are skipped
                _path_time = datetime.fromtimestamp(os.path.getctime(_path))
                if _path_time + _dt > _now:
                    continue

                # Calculate lifetime
                _lifetime = _now - _path_time
            except:
                logger.error(
                    "@JManager - Unable to extract job change time: %s." %
                    _jid, exc_info=True)
                continue

            # Append to the job table
            _job_table.append((_lifetime, _job))

        # Revers sort the table - oldest first
        _job_table = sorted(_job_table, reverse=True)
        # We are aiming at 80% quota utilisation
        _water_mark = _quota * 0.8
        if full:  # Remove all possible jobs
            _water_mark = 0
        # Remove oldest jobs first until water mark is reached
        for _item in _job_table:
            try:
                _job = _item[1]
                _job.delete()
                _service.remove_job_proxy(_job)
                logger.debug("@JManager - Job garbage collected: %s." %
                             _job.id)
            except:
                logger.warning("@JManager - unable schedule job for removal.",
                               exc_info=True)
            if _service.current_size < _water_mark:
                break

        # Hard quota is set at 130% of quota
        # If hard quota is exceed no new jobs can be submitted until disk space
        # is actually freed by check_deleted_jobs ...
        if _service.real_size > _quota * 1.3:
            logger.error("@JManager - Hard quota reached for service: %s" %
                         service)
            return False

        if _start_size != _service.current_size:
            logger.info(
                "Garbage collect reclaimed %s MB of disk space." %
                ((_start_size - _service.current_size) / 1000000.0)
            )

        if _service.current_size + _delta < _quota:
            return True

        return False

    def submit(self, job):
        """
        Generate job related scripts and submit them to selected scheduler.

        :param job: The Job object to submit.
        :return: True on success, False otherwise.
        """
        # During validation default values are set in Job.valid_data
        # Now we can access scheduler selected for current Job
        _scheduler = self.schedulers[job.valid_data['CIS_SCHEDULER']]
        # Ask scheduler to generate scripts and submit the job
        if _scheduler.generate_scripts(job):
            if _scheduler.chain_input_data(job):
                return _scheduler.submit(job)

        return False

    def shutdown(self):
        """
        Stop all running jobs.
        """
        for _job in JobStore.values():
            _state = _job.get_state()
            if _state in ('done', 'failed', 'aborted', 'killed'):
                continue
            if _state in ('queued', 'running'):
                _scheduler = self.schedulers[_job.valid_data['CIS_SCHEDULER']]
                _scheduler.stop(_job, 'Server shutdown', ExitCodes.Shutdown)
            else:
                _job.finish('Server shutdown', state='killed',
                            exit_code=ExitCodes.Shutdown)

        time.sleep(conf.config_shutdown_time)
        self.check_cleanup()

        for _job in JobStore.values():
            _state = _job.get_state()
            if _state in ('done', 'failed', 'aborted', 'killed'):
                continue
            else:
                _job.finish('Server shutdown', state='killed',
                            exit_code=ExitCodes.Shutdown)
                _job.exit()

    def stop(self):
        """
        Pause the queue. New jobs will not be processed.
        """
        self.__queue_running = False

    def start(self):
        """
        Restart the queue.
        """
        self.__queue_running = True

    def run(self):
        """
        Main loop of JobManager.

        * Check for new jobs - submit them if found,
        * Check for finished jobs - retrive output if found,
        * Check for jobs exceeding their life time - mark for removal,
        * Check for jobs to be removed - delete all related resources.
        """

        while(1):
            time.sleep(conf.config_sleep_time)
            if self.__queue_running:
                self.check_new_jobs()
            self.check_running_jobs()
            self.check_job_kill_requests()
            self.check_cleanup()
            self.check_old_jobs()
            self.check_deleted_jobs()
