#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Main module of CISAppServer. Responsible for job management.
"""

import os
import sys
import shutil
import time
import logging
import threading
from datetime import datetime, timedelta

from Tools import Validator, ValidatorInputFileError, ValidatorError, \
        CisError, PbsScheduler, SshScheduler, rmtree_error
from Config import conf, VERBOSE, ExitCodes
from Jobs import Job, JobState, StateManager
from DataStore import ServiceStore, SchedulerStore

version = "0.9"

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
        # Make sure state manager is initialized
        StateManager.init()

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

        # Main loop run guard
        self.__running = True
        self.__terminate = False
        self.__reload_config = False
        # State of the job queue
        self.__queue_running = True
        # Size of service output data last time quota was exceeded
        self.__last_service_size = {}
        # Warning counter - quota
        self.__w_counter_quota = {}
        for _s in ServiceStore.keys():
            self.__w_counter_quota[_s] = 0
        # Warning counter - slots
        self.__w_counter_slots = {}
        for _s in ServiceStore.keys():
            self.__w_counter_slots[_s] = 0
        # Time stamp for the last iteration
        self.__time_stamp = datetime.utcnow()
        # Thread list
        self.__thread_list_submit = []
        self.__thread_list_cleanup = []
        self.__thread_count_submit = 0
        self.__thread_count_cleanup = 0

    def clear(self):
        ServiceStore.clear()
        SchedulerStore.clear()
        del self.validator

    def check_new_jobs(self):
        """
        Check for new job files in the queue directory.

        If found try to submit them to selected job scheduler.
        """

        logger.log(VERBOSE, '@JManager - Check for new jobs.')

        # Reset unit of work timer
        self.__start_unit_timer()

        _now = datetime.utcnow()
        # Wait timeout
        _dt = timedelta(seconds=conf.config_wait_time)
        # Max wait timeout for job submission (e.g. for input upload)
        _dt_final = timedelta(hours=conf.service_max_lifetime)

        logger.log(VERBOSE, '@JManager - Run query.')
        # Count current active jobs
        try:
            _active_count = StateManager.get_active_job_count()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        # Available job slots
        _new_slots = conf.config_max_jobs - _active_count

        logger.log(VERBOSE, '@JManager - Free job slots: %s.', _new_slots)

        _stat_out = os.statvfs(conf.gate_path_output)
        _hard_quota = _stat_out.f_frsize * _stat_out.f_bavail

        # Available job slots per service
        #_service_slots = {}
        #for (_service_name, _service) in ServiceStore.items():
            # Count current active jobs
            #_service_active_count = StateManager.get_active_job_count(
            #        service=_service_name)
            # Available job slots
            #_service_slots[_service_name] = _service.config['max_jobs'] - \
            #        _service_active_count

        # Available job slots per service
        try:
            _counters = StateManager.get_active_service_counters()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return
        _service_jobs = { _key : 0 for _key in ServiceStore.keys() }
        for (_count, _key) in _counters:
            _service_jobs[_key] = _count

        # Available quota per service
        try:
            _counters = StateManager.get_quota_service_counters()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return
        _service_quota = {
                _key : _service.config['quota'] for \
                        _key, _service in ServiceStore.items()
                }
        for (_count, _key) in _counters:
            _service_quota[_key] -= _count

        # Available job slots
        _service_slots = {}
        for (_service_name, _service) in ServiceStore.items():
            _service_slots[_service_name] = _service.config['max_jobs'] - \
                    _service_jobs[_service_name]

        logger.log(VERBOSE, '@JManager - Free service slots: %s.', _service_slots)

        _i = 0
        _j = 0
        _batch = []
        try:
            _job_list = StateManager.get_new_job_list()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return
        logger.debug("Detected %s new jobs", len(_job_list))
        for _job in _job_list:
            if _job.get_flag(JobState.FLAG_DELETE):
                continue

            # Check for job slots left
            if _i >= _new_slots:
                break

            # Check for unit of work time left
            if not self.__check_unit_timer():
                break

            # Do not exceed max thread limit
            if len(self.__thread_list_submit) >= conf.config_max_threads:
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
                        logger.log(VERBOSE,
                                '@JManager - Job %s in wait state. End: %s, '
                                'Now: %s.', _job.id(), _wait_time, _now)
                        continue
                    else:
                        logger.log(VERBOSE, '@JManager - Job %s wait finished.',
                                _job.id())
                        if _wait_input:
                            _job.set_flag(JobState.FLAG_WAIT_INPUT, remove=True)
                        if _wait_quota:
                            _job.set_flag(JobState.FLAG_WAIT_QUOTA, remove=True)
            except:
                logger.log(VERBOSE, '@JManager - Wait flag extraction failed '
                        'for job %s.', _job.id(), exc_info=True)
                # Let's ignore exceptions and treat such jobs as without wait
                # flags
                pass

            logger.debug('@JManager - Detected new job %s.', _job.id())
            _service_name = _job.status.service
            _service = ServiceStore[_service_name]
            # Check available service slots
            if _service_slots[_service_name] <= 0:
                # Limit the number of warning messages
                if self.__w_counter_slots[_service_name] == 0:
                    logger.warning(
                        "@JManager - All job slots in use for service %s." %
                        _service_name
                    )
                if self.__w_counter_slots[_service_name] > 9999:
                    logger.error(
                        "@JManager - All job slots in use for service %s. "
                        "Message repeated 10000 times." %
                        _service_name
                    )
                    self.__w_counter_slots[_service_name] = 0
                else:
                    self.__w_counter_slots[_service_name] += 1
                continue
            else:
                self.__w_counter_slots[_service_name] = 0
            # @TODO should be moved?? from here for batch submits??
            # Check the service quota
            if _service_quota[_service_name] < _service.config['job_size'] or \
                    _hard_quota < _service.config['job_size']:
                logger.warning(
                    "@JManager - Quota for service %s exceeded." %
                    _service_name
                )
                # Flag the job to wait (no need to check the quota every tick)
                _job.set_flag(JobState.FLAG_WAIT_QUOTA)
                _job.status.wait_time = datetime.utcnow()
                continue

            _batch.append(_job)
            _service_slots[_service_name] -= 1  # Mark slot as used
            _service_quota[_service_name] -= _service.config['job_size']
            _hard_quota -= _service.config['job_size']
            _j += 1
            _i += 1  # Mark slot as used

            logger.debug("Batch size: %s / %s.", _j, conf.config_batch_jobs)
            if _j >= conf.config_batch_jobs:
                self.batch_submit(_batch)
                _j = 0
                _batch = []
                # Run batch select to refresh ORM state in one go after commit
                # in batch_submit. Otherwise a SELECT is issued for each job.
                # @TODO test if this is required
                try:
                    StateManager.get_job_list("waiting")
                except:
                    logger.error('Unable to contact with the DB.', exc_info=True)
                    return

        if len(_batch):
            self.batch_submit(_batch)

    def check_running_jobs(self):
        """
        Check status of running jobs.

        Finished jobs will be marked for finalisation
        """

        logger.log(VERBOSE, '@JManager - Check state of running jobs.')

        # Loop over supported schedulers
        for _sname, _scheduler in SchedulerStore.items():
            try:
                _jobs = StateManager.get_job_list(scheduler=_sname)
            except:
                logger.error('Unable to contact with the DB.', exc_info=True)
                continue
            _jobs_active = []
            for _job in _jobs:
                # Scheduler can change state for only running and waiting jobs.
                # Disregard the rest.
                _state = _job.get_state()
                if _state == 'closing' or _state == 'cleanup' or \
                        _state == 'processing':
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

        logger.log(VERBOSE, '@JManager - Check for jobs marked for cleanup.')

        # Reset unit of work timer
        self.__start_unit_timer()

        _i = 0
        _batch = []
        try:
            _job_list = StateManager.get_job_list("closing")
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        for _job in _job_list:
            # Check for unit of work time left
            if not self.__check_unit_timer():
                break

            # Do not exceed max thread limit
            if len(self.__thread_list_cleanup) >= conf.config_max_threads:
                break

            logger.debug('@JManager - Detected cleanup job %s.', _job.id())

            # Check for valid input data
            if not _job.get_exit_state():
                _job.die("@JManager - Job %s in closing state yet no exit "
                         "state defined." % _job.id())
                continue

            # Run cleanup in separate threads
            _batch.append(_job)
            _i += 1

            if _i >= conf.config_batch_jobs:
                self.batch_cleanup(_batch)
                _i = 0
                _batch = []

        if len(_batch):
            self.batch_cleanup(_batch)

    def check_job_kill_requests(self):
        """
        Check for job kill requests.

        If found and job is still running kill it.
        """

        logger.log(VERBOSE, '@JManager - Check for kill requests.')

        try:
            _job_list = StateManager.get_job_list(flag=JobState.FLAG_STOP)
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        for _job in _job_list:
            logger.debug('@JManager - Detected job marked for a kill: %s' %
                         _job.id())

            # Wait for the job submission thread to finish
            if _job.get_state() == 'processing':
                continue
            # Stop if it is running
            if _job.get_state() == 'running' or \
                    _job.get_state() == 'queued':
                try:
                    SchedulerStore[_job.status.scheduler].stop(
                        _job, 'User request', ExitCodes.UserKill
                    )
                except CisError as e: # Temporary job stop problem
                    continue
                except:
                    _job.die("@PBS - Unable to terminate job %s." %
                            _job.id(), exc_info=True)
                    continue
            elif _job.get_state() == 'waiting':
                _job.finish('User request', 'killed', ExitCodes.UserKill)
            else:
                logger.warning("@JManager - Cannot kill job %s. "
                               "It is already finished.", _job.id())

            # Remove the kill mark
            try:
                _job.set_flag(JobState.FLAG_STOP, remove=True)
            except:
                logger.error("Cannot remove kill flag for job %s.", _job.id(),
                             exc_info=True)

    def check_deleted_jobs(self):
        """
        Check for jobs marked for removal.

        If found remove all resources related to the job. If a job is still
        running kill it.
        """

        logger.log(VERBOSE, '@JManager - Check for delete requests.')

        for _job in StateManager.get_job_list(flag=JobState.FLAG_DELETE):
            _jid = _job.id()
            logger.debug('@JManager - Detected job marked for deletion: %s' %
                         _jid)

            # Stop if it is running
            if _job.get_state() in ('running', 'queued'):
                try:
                    SchedulerStore[_job.status.scheduler].stop(
                        _job, 'User request', ExitCodes.Delete
                    )
                except CisError as e: # Temporary job stop problem
                    continue
                except:
                    _job.die("@PBS - Unable to terminate job %s." %
                            _job.id(), exc_info=True)
                continue

            # Wait for job submission or finalisation threads to finish
            if _job.get_state() in ('processing', 'cleanup', 'closing'):
                continue

            # Remove the output directory and its contents
            _output = os.path.join(conf.gate_path_output, _jid)
            _dump = os.path.join(conf.gate_path_dump, _jid)
            try:
                if os.path.isdir(_output):
                    shutil.move(_output, _dump)
                    shutil.rmtree(_dump, onerror=rmtree_error)
            except:
                logger.error("Cannot remove job output %s.", _jid,
                             exc_info=True)

            # Delete the job
            try:
                StateManager.delete_job(_job)
            except:
                logger.error("Cannot remove job %s.", _jid, exc_info=True)

            logger.info('@JManager - Job %s removed with all data.' %
                        _jid)

    def check_old_jobs(self):
        """Check for jobs that exceed their life time.

        If found mark them for removal."""

        logger.log(VERBOSE, '@JManager - Check for expired jobs.')

        for _job in StateManager.get_job_list():
            # Skip jobs already flagged for removal
            if _job.get_flag(JobState.FLAG_DELETE):
                continue

            # Skip not affected states
            _state = _job.get_state()
            if _state in ('new', 'waiting', 'processing', 'queued', 'closing',
                    'cleanup'):
                continue

            logger.log(VERBOSE, "Job %s, State:%s, Status:%s", _job.id(), _state, str(_job.status))

            # Check the MAX job lifetime defined by the service
            _delete_dt = ServiceStore[_job.status.service].config['max_lifetime']
            if _delete_dt == 0:
                continue

            _now = datetime.utcnow()
            _dt = timedelta(hours=_delete_dt)
            _path_time = None
            try:
                if _state in ('done', 'failed', 'killed', 'aborted'):
                    _path_time = _job.status.stop_time
                elif _state == 'running':  # For running jobs use the MAX runtime
                    _path_time = _job.status.start_time
                    _delete_dt = \
                        ServiceStore[_job.status.service].config['max_runtime']
                    _dt = timedelta(hours=_delete_dt)
                else:  # Jobs in other states are not affected
                    continue

                logger.log(VERBOSE, "@JManager - Removal dt: %s", _dt)
                logger.log(VERBOSE, "@JManager - Job time: %s", _path_time)
                logger.log(VERBOSE, "@JManager - Current time: %s", _now)
                logger.log(VERBOSE, "@JManager - Time diff: %s",
                        (_path_time + _dt - _now))
                _path_time += _dt  # Calculate the removal time stamp
            except:
                logger.error(
                    "@JManager - Unable to extract job change time: %s." %
                    _job.id(), exc_info=True)
                continue

            # Was the removal time stamp already passed?
            if _path_time < _now:
                logger.info("@JManager - Job reached storage time limit. "
                            "Sheduling for removal.")
                _job.delete()

    def report_jobs(self):
        """
        Log the current queue state.
        """

        _results = StateManager.get_job_state_counters()
        _states = { _key : 0 for _key in conf.service_states }
        for (_count, _key) in _results:
            _states[_key] = _count

        logger.info("Jobs - w:%s, p:%s, q:%s, r:%s, s:%s, c:%s, d:%s, f:%s, "
                "a:%s, k:%s.", _states['waiting'], _states['processing'],
                _states['queued'], _states['running'], _states['closing'],
                _states['cleanup'], _states['done'], _states['failed'],
                _states['aborted'], _states['killed'])

    def check_finished_threads(self):
        """Check for cleanup threads that finished execution.

        If found finalise them."""

        logger.log(VERBOSE, '@JManager - Check for finished cleanup threads.')

        _clean = True

        # Remove finished threads
        for _thread in self.__thread_list_submit:
            if not _thread.is_alive():
                _thread.join()
                self.__thread_list_submit.remove(_thread)
                _clean = False
                logger.debug("@JManager - Removed finished subthread.")
        for _thread in self.__thread_list_cleanup:
            if not _thread.is_alive():
                _thread.join()
                self.__thread_list_cleanup.remove(_thread)
                _clean = False
                logger.debug("@JManager - Removed finished subthread.")
        if not _clean:
            StateManager.expire_session()

    def collect_garbage(self, full=False):
        """
        Check if service quota is not exceeded. If yes remove oldest finished
        jobs.

        :param full: If True force garbage collection even if disk usage is
            not above alloted quota. In addition removes all jobs older than
            min job life time.
        """

        logger.log(VERBOSE, '@JManager - Garbage collect.')

        # Available quota per service
        try:
            _counters = StateManager.get_quota_service_counters()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return
        _service_usage = { _key : 0 for _key in ServiceStore.keys() }
        _service_quota = { _key : _service.config['quota'] \
                for _key, _service in ServiceStore.items() }
        for (_count, _key) in _counters:
            _service_usage[_key] = _count

        logger.log(VERBOSE, 'Service quota utilisation: %s.', _service_usage)
        logger.log(VERBOSE, 'Service quota limits: %s.', _service_quota)

        for (_service_name, _service) in ServiceStore.items():
            _usage = _service_usage[_service_name]
            _start_size = _usage
            _delta = _service.config['job_size']
            _quota = _service.config['quota']

            if _usage + _delta < _quota and not full:
                continue

            logger.log(
                       VERBOSE,
                       "Service %s space utilisation exceeds quota: %s",
                       _service_name,
                       _quota - _usage - _delta
                      )

            _job_table = []  # List of tuples (lifetime, job)
            for _job in StateManager.get_job_list(service=_service_name):
                _jid = _job.id()
                if _job.get_flag(JobState.FLAG_DELETE):
                    continue
                # Get protection interval
                _protect_dt = _service.config['min_lifetime']
                _dt = timedelta(hours=_protect_dt)
                _state = _job.get_state()
                _now = datetime.utcnow()
                try:
                    # We consider only jobs that could have produced output
                    if _state in ['done', 'failed', 'killed', 'aborted']:
                        _time = _job.status.stop_time
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

                logger.log(VERBOSE,
                        "Job available for garbage collection - "
                        "ID:%s, lifetime %s",
                        _jid, _lifetime)
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
                    _usage -= _job.get_size()
                    logger.debug("@JManager - Job garbage collected: %s." %
                                 _jid)
                except:
                    logger.warning("@JManager - unable schedule job for removal.",
                                   exc_info=True)
                if _usage + _delta < _water_mark:
                    break

            if _start_size != _usage:
                logger.info(
                    "Garbage collect reclaimed %s MB of disk space." %
                    ((_start_size - _usage) / 1000000.0)
                )

    def batch_submit(self, batch):
        _job_ids = []
        # Run submit in separate threads
        for _job in batch:
            try:
                _job.processing()
                _job_ids.append(_job.id())
            except:
                _job.die("Unable to change state.")

        if not self.__commit():
            return

        try:
            logger.debug('@JManager - Start submit thread')
            # The sub thread cannot use the same Job instance or the same
            # DB session as the main one. Pass the JobID istead so that the
            # thread can create its own instances.

            _thread = threading.Thread(
                    target=self.submit, args=(_job_ids,),
                    name="Submit_%s"%self.__thread_count_submit)
            _thread.start()
            self.__thread_list_submit.append(_thread)
            logger.debug("@JManager - Submit thread %s started.",
                         self.__thread_count_submit)
            self.__thread_count_submit += 1
        except:
            logger.error("@JManager - Unable to start submit thread %s",
                         self.__thread_count_submit, exc_info=True)

    def submit(self, job_ids):
        """
        Generate job related scripts and submit them to selected scheduler.

        :param job: The Job object to submit.
        :return: True on success, False otherwise.
        """
        logger.debug("Submit batch of %s jobs.", len(job_ids))

        try:
            _session = StateManager.new_session(autoflush=False)
            # Do an eager load so that no more SELECT statements are issued.
            # This should prevent issuing update statements on job changes
            # untli commit is issued and will prevent the DB lock.
            _jobs = StateManager.get_job_list_byid(job_ids, session=_session, full=True)
        except:
            logger.error("@JobManager - Unable to connect to DB.",
                         exc_info=True)
            return

        for _job in _jobs:
            _jid = _job.id()

            # Validate input
            try:
                self.validator.validate(_job)
            except ValidatorInputFileError as e:
                # The input file is not available yet. Flag the job to wait.
                _job.set_flag(JobState.FLAG_WAIT_INPUT)
                _job.status.wait_time = datetime.utcnow()
                _job.wait()
                logger.log(VERBOSE, '@JManager - Job flagged to wait for input.')
                continue
            except ValidatorError as e:
                _job.die("@JManager - Job %s validation failed." % _job.id(),
                         exc_info=True, err=False, exit_code=ExitCodes.Validate)
                continue
            except:
                _job.die("@JManager - Job %s validation failed." % _job.id(),
                         exc_info=True, exit_code=ExitCodes.Validate)
                continue

            # During validation default values are set in Job.valid_data
            # Now we can access scheduler selected for current Job
            try:
                _scheduler = SchedulerStore[_job.status.scheduler]
            except:
                logger.error("@JobManager - Unable to obtain scheduler and "
                             "service instance.", exc_info=True)
                continue
            # Ask scheduler to generate scripts and submit the job
            try:
                if _scheduler.generate_scripts(_job):
                    if _scheduler.chain_input_data(_job):
                        if _scheduler.submit(_job):
                            _job.queue()
            except:
                logger.error("@JobManager - Unable to submit job.", exc_info=True)
                continue

        self.__commit(_session)
        logger.debug("Job submit thread finished.")

    def batch_cleanup(self, batch):
        _job_ids = []
        # Run submit in separate threads
        for _job in batch:
            try:
                _job.cleanup()
                _job_ids.append(_job.id())
            except:
                _job.die('Unable to set job state.')

        logger.debug('Change jobs state and commit to the DB.')
        # Mark job as in processing state and commit to DB
        if not self.__commit():
            return

        try:
            logger.debug('Start cleanup thread')
            # The sub thread cannot use the same Job instance or the same
            # DB session as the main one. Pass the JobID istead so that the
            # thread can create its own instances.

            _thread = threading.Thread(
                    target=self.cleanup, args=(_job_ids,),
                    name="Cleanup_%s"%self.__thread_count_cleanup)
            _thread.start()
            self.__thread_list_cleanup.append(_thread)
            logger.debug("@JManager - Cleanup thread %s started.",
                         self.__thread_count_cleanup)
            self.__thread_count_cleanup += 1
        except:
            logger.error("@JManager - Unable to start cleanup thread %s",
                         self.__thread_count_cleanup, exc_info=True)

    def cleanup(self, job_ids):
        """
        """
        logger.debug("Cleanup batch of %s jobs.", len(job_ids))

        try:
            _session = StateManager.new_session(autoflush=False)
            _jobs = StateManager.get_job_list_byid(job_ids, session=_session, full=True)
        except:
            logger.error("@JobManager - Unable to connect to DB.",
                         exc_info=True)
            return

        for _job in _jobs:
            _jid = _job.id()

            try:
                _scheduler = SchedulerStore[_job.status.scheduler]
                if _job.get_exit_state() == 'aborted':
                    _scheduler.abort(_job)
                else:
                    _scheduler.finalise(_job)
            except:
                logger.error("Job %s cleanup not finished with error.",
                             _jid, exc_info=True)
                continue

        self.__commit(_session)
        logger.debug("Job cleanup thread finished.")

    def shutdown(self):
        """
        Stop all running jobs.
        """
        logger.info("Starting shutdown")

        # Make sure that jobs ready for cleanup start processing immediatelly
        #StateManager.expire_session()
        #self.check_cleanup()
        # Wait for processing threads to finish
        #time.sleep(conf.config_shutdown_time)
        #StateManager.expire_session()
        time.sleep(conf.config_shutdown_time)
        self.check_running_jobs()
        self.check_cleanup()

        # Kill running, queued and waiting jobs
        if self.__terminate:
            for _job in StateManager.get_job_list():
                _state = _job.get_state()
                if _state in ('queued', 'running'):
                    _scheduler = SchedulerStore[_job.status.scheduler]
                    try:
                        _scheduler.stop(_job, 'Server shutdown', ExitCodes.Shutdown)
                    except CisError as e: # Temporary job stop problem
                        continue
                    except:
                        _job.die("@PBS - Unable to terminate job %s." %
                                _job.id(), exc_info=True)
            # Wait for PBS to kill the jobs
            time.sleep(conf.config_shutdown_time)

        # Check for jobs that got killed and lunch cleanup
        self.check_running_jobs()
        self.check_cleanup()
        time.sleep(conf.config_shutdown_time)

        for _thread in self.__thread_list_submit:
            # There is no way to safely kill a python thread that is performing
            # a blocking IO. Call a join with timeout in hope that the cleanup
            # finishes by then. Switching to multiprocessing.Process is not an
            # option as we want to share the SSH spur context.
            _thread.join(conf.config_shutdown_time)
            self.__thread_list_submit.remove(_thread)
            logger.debug("@JManager - Removed subthread.")
        for _thread in self.__thread_list_cleanup:
            # There is no way to safely kill a python thread that is performing
            # a blocking IO. Call a join with timeout in hope that the cleanup
            # finishes by then. Switching to multiprocessing.Process is not an
            # option as we want to share the SSH spur context.
            _thread.join(conf.config_shutdown_time)
            self.__thread_list_cleanup.remove(_thread)
            logger.debug("@JManager - Removed subthread.")
        StateManager.expire_session()

        if self.__terminate:
            # Force killed state on leftovers
            for _job in StateManager.get_job_list():
                _state = _job.get_state()
                if _state in ('done', 'failed', 'aborted', 'killed'):
                    continue
                else:
                    _job.finish('Server shutdown', state='killed',
                                exit_code=ExitCodes.Shutdown)
                    _job.exit()

        # Pass the final state to AppGw
        self.__commit()

        logger.info("Shutdown complete")
        logging.shutdown()
        sys.exit(0)

    def stop(self):
        self.__running = False

    def terminate(self):
        self.__running = False
        self.__terminate = True

    def reload_config(self):
        self.__reload_config = True

    def stop_queue(self):
        """
        Pause the queue. New jobs will not be processed.
        """
        self.__queue_running = False

    def start_queue(self):
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

        import yappi
        yappi.start()

        _n = 0
        while(self.__running):
            # Reload config if requested
            if self.__reload_config:
                conf.load(conf.config_file)
                self.clear()
                self.init()
                logger.info("Reload complete")
                self.__reload_config = False

            # Calculate last iteration execute time
            _exec_time = (datetime.utcnow() - self.__time_stamp).total_seconds()
            # Calculate required sleep time
            _dt = conf.config_sleep_time - _exec_time
            if _dt < 0:
                if _dt + conf.config_sleep_time < 0:
                    logger.error("@JobManager - Main loop execution behind "
                                 "schedule by %s seconds.", _dt)
            else:
                # Sleep
                time.sleep(_dt)
            # Store new time stamp
            self.__time_stamp = datetime.utcnow()

            # Execute loop
            if self.__queue_running:  # Do not process queue in pause state
                self.check_new_jobs()
            self.check_running_jobs()
            self.check_job_kill_requests()
            self.check_cleanup()
            # Do not collect garbage every iteration
            if _n >= conf.config_garbage_step:
                self.collect_garbage()
                self.check_old_jobs()
                if conf.log_level == 'DEBUG' or conf.log_level == 'VERBOSE':
                    self.report_jobs()
                _n = 0
            self.check_finished_threads()
            self.check_deleted_jobs()
            self.__commit()
            StateManager.poll_gw()
            _n += 1

        _stat = open("/tmp/AppServer.profile","w")
        yappi.get_func_stats().print_all(out=_stat)
        yappi.get_thread_stats().print_all(out=_stat)
        _stat.close()
        logger.debug("Main Loop End")

        self.shutdown()

    def __check_unit_timer(self):
        _exec_time = (datetime.utcnow() - self.__time_stamp_unit).total_seconds()
        return (_exec_time < (2.0 * conf.config_sleep_time / 3.0))

    def __start_unit_timer(self):
        self.__time_stamp_unit = datetime.utcnow()

    def __commit(self, session=None):
        _commit = False
        _i = 0
        while not _commit and _i < 5:
            try:
                StateManager.commit(session)
                if session is not None:
                    session.close()
                _commit = True
                logger.debug("Commit to DB successfull.")
            except:
                _i += 1
                if _i < 5:
                    logger.warning('Commit to DB failed - retry.', exc_info=True)
                else:
                    logger.error('Unable to commit changes to DB.', exc_info=True)
            time.sleep(0.2)

        return _commit

