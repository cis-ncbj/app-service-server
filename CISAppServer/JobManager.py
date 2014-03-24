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

from Tools import Validator, ValidatorInputFileError, ValidatorError, PbsScheduler, SshScheduler, rmtree_error
from Config import conf, verbose, ExitCodes
from Jobs import Job, StateManager
from DataStore import ServiceStore, JobStore, SchedulerStore

version = "0.5"

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
        self.validator = Validator()  #: Validator instance
        for _scheduler in conf.config_schedulers:
            if _scheduler == 'pbs':
                SchedulerStore[_scheduler] = PbsScheduler()
            elif _scheduler == 'ssh':
                SchedulerStore[_scheduler] = SshScheduler()

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

    def clear(self):
        ServiceStore.clear()
        SchedulerStore.clear()
        del self.validator

    def check_new_jobs(self):
        """
        Check for new job files in the queue directory.

        If found try to submit them to selected job scheduler.
        """

        verbose('@JManager - Check for new jobs.')

        _now = datetime.now()
        # Wait timeout
        _dt = timedelta(seconds=conf.config_wait_time)
        # Max wait timeout for job submission (e.g. for input upload)
        _dt_final = timedelta(hours=conf.service_max_lifetime)

        # Count current active jobs
        _queue_count = StateManager.get_job_count("queued")
        _run_count = StateManager.get_job_count("running")
        _close_count = StateManager.get_job_count("closing")
        _clean_count = StateManager.get_job_count("cleanup")

        # Available job slots
        _new_slots = conf.config_max_jobs - _queue_count - _run_count - \
            _close_count - _clean_count

        _i = 0
        for _job in StateManager.get_new_job_list():
            if _job.get_flag(JobState.FLAG_DELETE):
                continue

            # Check for job slots left
            if _i >= _new_slots:
                break

            # Check if the job was flagged to wait and skip it if the wait
            # timeout did not expire yet.
            try:
                _wait_input = _job.get_flag(JobState.FLAG_WAIT_INPUT)
                _wait_quota = _job.get_flag(JobState.FLAG_WAIT_QUOTA)

                # Check max wait time for input timeouts
                if _wait_input:
                    _submit_time = _job.status.submit_time
                    _submit_time += _dt_final

                    if _submit_time < _now:
                        _job.die("@JManager - Input file not available for "
                                 "job %s. Time out." % _job.id())
                        continue

                # Check wait timeout
                if _wait_input or _wait_quota:
                    _wait_time = _job.status.wait_time
                    _wait_time += _dt
                    if _wait_time > _now:
                        verbose('@JManager - Job %s in wait state. End: %s, Now: %s.' % (_job.id(), _wait_time, _now))
                        continue
                    else:
                        verbose('@JManager - Job %s wait finished.' % _job.id())
                        if _wait_input:
                            _job.set_flag(JobState.FLAG_WAIT_INPUT, remove=True)
                        if _wait_quota:
                            _job.set_flag(JobState.FLAG_WAIT_QUOTA, remove=True)
            except:
                verbose('@JManager - Wait flag extraction failed for job %s.' % _job.id(),
                        exc_info=True)
                # Let's ignore exceptions and treat such jobs as without wait
                # flags
                pass

            logger.debug('@JManager - Detected new job %s.' % _job.id())

            # Validate input
            try:
                self.validator.validate(_job)
            except ValidatorInputFileError as e:
                # The input file is not available yet. Flag the job to wait.
                _job.set_flag(JobState.FLAG_WAIT_INPUT)
                _job.status.wait_time = datetime.now()
                continue
            except ValidatorError as e:
                _job.die("@JManager - Job %s validation failed." % _job.id(),
                         exc_info=True, err=False, exit_code=ExitCodes.Validate)
                continue
            except:
                _job.die("@JManager - Job %s validation failed." % _job.id(),
                         exc_info=True, exit_code=ExitCodes.Validate)
                continue
            verbose('@JManager - Check for new jobs.')

            # Check the service quota
            _service_name = _job.status.service
            _service = ServiceStore[_service_name]
            if _service.is_full():
                # Limit the number of warning messages
                if self.__w_counter_quota[_service_name] < \
                   6 * 60 * 60 / conf.config_sleep_time:
                    self.__w_counter_quota[_service_name] += 1
                    if _service.current_size != \
                       self.__last_service_size[_service_name]:
                        logger.warning(
                            "@JManager - Quota for service %s exceeded." %
                            _service_name
                        )
                        self.__last_service_size[_service_name] = \
                            _service.current_size
                else:
                    logger.error(
                        "@JManager - Quota for service %s exceeded. "
                        "Message repeated 100 times." %
                        _service_name
                    )
                    self.__w_counter_quota[_service_name] = 0
                # Flag the job to wait (no need to check the quota every tick)
                _job.set_flag(JobState.FLAG_WAIT_QUOTA)
                _job.status.wait_time = datetime.now()
                continue
            else:
                self.__last_service_size[_service_name] = \
                    _service.current_size
                self.__w_counter_quota[_service_name] = 0

            try:
                if self.submit(_job):
                    _job.queue()
                    _service.add_job_proxy(_job)
                # Submit can return False when queue is full. Do not terminate
                # job here so it can be resubmitted next time. If submission
                # failed scheduler should have set job state to Aborted anyway.
            except:
                _job.die("@JManager - Cannot start job %s." % _job.id(),
                         exc_info=True)
                continue

            _i += 1

    def check_running_jobs(self):
        """
        Check status of running jobs.

        Finished jobs will be marked for finalisation
        """

        verbose('@JManager - Check state of running jobs.')

        # Loop over supported schedulers
        for _sname, _scheduler in SchedulerStore.items():
            _jobs = StateManager.get_scheduler_list(_sname)
            _jobs_active = []
            for _job in _jobs:
                # Scheduler can change state for only running and waiting jobs.
                # Disregard the rest.
                _state = _job.get_state()
                if _state == 'closing' or _state == 'cleanup':
                    continue
                elif _state == 'running' or _state == 'queued':
                    _jobs_active.append(_job)
                else:
                    _job.die("@JManager - job state %s not allowed while in "
                             "scheduler queue" % _state)

            # Ask the scheduler to run the update
            _scheduler.update(_jobs_active)

    def check_cleanup(self):
        """
        Check jobs marked for cleanup.

        Starts the cleanup for the jobs in separate threads.
        """

        verbose('@JManager - Check for jobs marked for cleanup.')

        for _job in StateManager.get_job_list("closing"):
            logger.debug('@JManager - Detected cleanup job %s.' % _job.id())

            # Check for valid input data
            if not _job.get_exit_state():
                _job.die("@JManager - Job %s in closing state yet no exit "
                         "state defined." % _job.id())
                continue

            _scheduler = SchedulerStore[_job.status.scheduler]
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
                                 "for job %s" % _job.id())
                else:
                    _thread = threading.Thread(
                        target=_scheduler.finalise, args=(_job,))
                    _thread.start()
                    self.__thread_list.append(_thread)
                    logger.debug("@JManager - Finalise cleanup thread started "
                                 "for job %s" % _job.id())
            except:
                logger.error("@JManager - Unable to start cleanup thread "
                             "for job %s" % _job.id(), exc_info=True)

    def check_job_kill_requests(self):
        """
        Check for job kill requests.

        If found and job is still running kill it.
        """

        verbose('@JManager - Check for kill requests.')

        for _job in StateManager.get_job_list(flag=JobState.FLAG_STOP):
            logger.debug('@JManager - Detected job marked for a kill: %s' %
                         _job.id())

            # Stop if it is running
            if _job.get_state() == 'running' or \
                    _job.get_state() == 'queued':
                SchedulerStore[_job.status.scheduler].stop(
                    _job, 'User request', ExitCodes.UserKill
                )
            elif _job.get_state() == 'waiting':
                _job.finish('User request', 'killed', ExitCodes.UserKill)
            else:
                logger.warning("@JManager - Cannot kill job %s. "
                               "It is already finished." % _job.id())

            # Remove the kill mark
            try:
                _job.set_flag(JobState.FLAG_STOP, remove=True)
            except:
                logger.error("Cannot remove kill flag for job %s." % _job.id(),
                             exc_info=True)

    def check_deleted_jobs(self):
        """
        Check for jobs marked for removal.

        If found remove all resources related to the job. If a job is still
        running kill it.
        """

        verbose('@JManager - Check for delete requests.')

        for _job in StateManager.get_job_list(flag=JobState.FLAG_DELETE):
            _jid = _job.id()
            logger.debug('@JManager - Detected job marked for deletion: %s' %
                         _jid)

            # Stop if it is running
            if _job.get_state() in ('running', 'queued'):
                SchedulerStore[_job.status.scheduler].stop(
                    _job, "User request", ExitCodes.Delete
                )
                continue

            if _job.get_state() in ('cleanup'):
                continue

            # Remove the output directory and its contents
            _output = os.path.join(conf.gate_path_output, _jid)
            _dump = os.path.join(conf.gate_path_dump, _jid)
            try:
                if os.path.isdir(_output):
                    shutil.move(_output, _dump)
                    shutil.rmtree(_dump, onerror=rmtree_error)
                    # Update service quota status
                    ServiceStore[_job.status.service].remove_job(_job)
            except:
                logger.error("Cannot remove job output %s." % _jid,
                             exc_info=True)

            # Delete the job
            try:
                StateManager.delete_job(_job)
            except:
                logger.error("Cannot remove job %s." % _jid, exc_info=True)

            logger.info('@JManager - Job %s removed with all data.' %
                        _jid)

    def check_old_jobs(self):
        """Check for jobs that exceed their life time.

        If found mark them for removal."""

        verbose('@JManager - Check for expired jobs.')

        for _job in StateManager.get_job_list():
            if _job.get_flag(JobState.FLAG_DELETE):
                continue

            _delete_dt = ServiceStore[_job.status.service].config['max_lifetime']
            if _delete_dt == 0:
                continue

            _state = _job.get_state()
            _now = datetime.now()
            _dt = timedelta(hours=_delete_dt)
            _path_time = None
            try:
                if _state in ('done', 'failed', 'killed', 'aborted'):
                    _path_time = _job.status.stop_time
                elif _state == 'running':
                    _path_time = _job.status.start_time
                    _delete_dt = \
                        ServiceStore[_job.status.service].config['max_runtime']
                    _dt = timedelta(hours=_delete_dt)
                else:
                    continue

                verbose("@JManager - Removal dt: %s" % _dt)
                verbose("@JManager - Job time: %s" % _path_time)
                verbose("@JManager - Current time: %s" % _now)
                verbose("@JManager - Time diff: %s" %
                        (_path_time + _dt - _now))
                _path_time += _dt
            except:
                logger.error(
                    "@JManager - Unable to extract job change time: %s." %
                    _job.id(), exc_info=True)
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

        verbose('@JManager - Garbage collect.')

        # Get Service instance
        _service = ServiceStore[service]
        _start_size = _service.current_size
        _delta = _service.config['job_size']
        _quota = _service.config['quota']

        # Check quota - size is stored in bytes while quota and delta in MBs
        if not _service.is_full() and not full:
            return

        _job_table = []  # List of tuples (lifetime, job)
        for _job in StateManager.get_job_list(service=service):
            _jid = _job.id()
            if _job.get_flag(JobState.FLAG_DELETE):
                continue
            # Get protection interval
            _protect_dt = _service.config['min_lifetime']
            _dt = timedelta(hours=_protect_dt)
            _state = _job.get_state()
            _now = datetime.now()
            try:
                # We consider only jobs that could have produced output
                if _state in ['done', 'failed', 'killed', 'aborted']:
                    _time = _jid.status.stop_time
                else:
                    continue

                # Jobs that are too young and are still in protection interval
                # are skipped
                if _time + _dt > _now:
                    continue

                # Calculate lifetime
                _lifetime = _now - _time
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
                             _job.id())
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
        _scheduler = SchedulerStore[job.status.scheduler]
        # Ask scheduler to generate scripts and submit the job
        if _scheduler.generate_scripts(job):
            if _scheduler.chain_input_data(job):
                return _scheduler.submit(job)

        return False

    def shutdown(self):
        """
        Stop all running jobs.
        """
        for _job in StateManager.get_job_list():
            _state = _job.get_state()
            if _state in ('done', 'failed', 'aborted', 'killed'):
                continue
            if _state in ('queued', 'running'):
                _scheduler = SchedulerStore[_job.status.scheduler]
                _scheduler.stop(_job, 'Server shutdown', ExitCodes.Shutdown)
            else:
                _job.finish('Server shutdown', state='killed',
                            exit_code=ExitCodes.Shutdown)

        time.sleep(conf.config_shutdown_time)
        self.check_cleanup()
        time.sleep(conf.config_shutdown_time)

        for _thread in self.__thread_list:
            # @TODO Kill the thread
            _thread.join()
            self.__thread_list.remove(_thread)
            logger.debug("@JManager - Removed finished cleanup thread.")

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

        _n = 0
        while(1):
            time.sleep(conf.config_sleep_time)
            if self.__queue_running:
                self.check_new_jobs()
            self.check_running_jobs()
            self.check_job_kill_requests()
            self.check_cleanup()
            if _n >= conf.config_garbage_step:
                for _service in ServiceStore.keys():
                    self.collect_garbage(_service)
                self.check_old_jobs()
                _n = 0
            self.check_deleted_jobs()
            StateManager.commit()
            _n += 1
