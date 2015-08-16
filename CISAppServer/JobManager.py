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
import multiprocessing
import cProfile

from datetime import datetime, timedelta

import Globals as G
from Config import conf, VERBOSE, ExitCodes
from Services import ValidatorInputFileError, ValidatorError, CisError
from Schedulers import rmtree_error
from Jobs import JobState

version = "0.9"

logger = logging.getLogger(__name__)


class JobManager(object):
    """
    Main calss of CISAppServer. It is responsible for job management.
    """

    def __init__(self):
        """
        Upon initialization sets up Validator and Scheduler interfaces.
        """
        self.init()

    def init(self):
        """
        Initialize JobManager. Creates new instances of Validator and
        Schedulers. Loads existing jobs from file system.

        Existing state is purged.
        """
        G.init()

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
        for _s in G.SERVICE_STORE:
            self.__w_counter_quota[_s] = 0
        # Warning counter - slots
        self.__w_counter_slots = {}
        for _s in G.SERVICE_STORE:
            self.__w_counter_slots[_s] = 0
        # Time stamp for the last iteration
        self.__time_stamp = datetime.utcnow()
        # Thread list
        self.__thread_lock = multiprocessing.Lock()
        self.__thread_pool_submit = multiprocessing.Pool(
                processes = conf.config_max_threads,
                initializer = worker_init,
                initargs=(conf, "SubmitWorker", self.__thread_lock)
                )
        self.__thread_pool_cleanup = multiprocessing.Pool(
                processes = conf.config_max_threads,
                initializer = worker_init,
                initargs=(conf, "CleanupWorker", self.__thread_lock)
                )
        self.__thread_list_submit = []
        self.__thread_list_cleanup = []

    def clear(self):
        # Push not commited changes to DB and expire local cache.
        G.STATE_MANAGER.check_commit()
        logger.debug("Closing subprocesses")
        self.__thread_pool_submit.close()
        self.__thread_pool_cleanup.close()
        self.__thread_pool_submit.join()
        self.__thread_pool_cleanup.join()
        self.__thread_pool_submit = None
        self.__thread_pool_cleanup = None
        logger.debug("Subprocesses closed")

        if self.__terminate:
            _job_list = []
            try:
                _job_list = G.STATE_MANAGER.get_job_list("closing")
            except:
                logger.error('Unable to contact with the DB.', exc_info=True)

            # Force killed state on leftovers
            for _job in _job_list:
                _state = _job.get_state()
                if _state in ('done', 'failed', 'aborted', 'killed'):
                    continue
                else:
                    _job.finish('Server shutdown', state='killed',
                                exit_code=ExitCodes.Shutdown)
                    _job.exit()

        # Pass the final state to AppGw
        G.STATE_MANAGER.check_commit()
        self.report_jobs()
        G.STATE_MANAGER.clear()
        G.SERVICE_STORE.clear()
        G.SCHEDULER_STORE.clear()


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
            _active_count = G.STATE_MANAGER.get_active_job_count()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        # Available job slots
        _new_slots = conf.config_max_jobs - _active_count

        logger.log(VERBOSE, 'Free job slots: %s.', _new_slots)

        _stat_out = os.statvfs(conf.gate_path_output)
        _hard_quota = _stat_out.f_frsize * _stat_out.f_bavail

        # Available job slots per service
        #_service_slots = {}
        #for (_service_name, _service) in G.SERVICE_STORE.items():
            # Count current active jobs
            #_service_active_count = G.STATE_MANAGER.get_active_job_count(
            #        service=_service_name)
            # Available job slots
            #_service_slots[_service_name] = _service.config['max_jobs'] - \
            #        _service_active_count

        #TODO Add available job slots per scheduler - flag the jobs to wait
        # Available job slots per service
        try:
            _counters = G.STATE_MANAGER.get_active_service_counters()
        except:
            logger.error('Unable to contact the DB.', exc_info=True)
            return
        _service_jobs = { _key : 0 for _key in G.SERVICE_STORE }
        for (_count, _key) in _counters:
            _service_jobs[_key] = _count

        # Available quota per service
        try:
            _counters = G.STATE_MANAGER.get_quota_service_counters()
        except:
            logger.error('Unable to contact the DB.', exc_info=True)
            return
        _service_quota = {
                _key : _service.config['quota'] for \
                        _key, _service in G.SERVICE_STORE.items()
                }
        for (_count, _key) in _counters:
            _service_quota[_key] -= _count

        # Available job slots
        _service_slots = {}
        for (_service_name, _service) in G.SERVICE_STORE.items():
            _service_slots[_service_name] = _service.config['max_jobs'] - \
                    _service_jobs[_service_name]

        logger.log(VERBOSE, '@JManager - Free service slots: %s.', _service_slots)

        _i = 0
        _j = 0
        _batch = []
        try:
            _job_list = G.STATE_MANAGER.get_new_job_list()
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
            #if len(self.__thread_list_submit) >= conf.config_max_threads:
            #    break

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
            _service = G.SERVICE_STORE[_service_name]
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
                    G.STATE_MANAGER.get_job_list("waiting")
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
        for _sname, _scheduler in G.SCHEDULER_STORE.items():
            try:
                _jobs = G.STATE_MANAGER.get_job_list(scheduler=_sname)
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
            try:
                _scheduler.update(_jobs_active)
            except:
                logger.error('Error occured while updating job states.', exc_info=True)

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
            _job_list = G.STATE_MANAGER.get_job_list("closing")
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        logger.debug("Found %s jobs ready for cleanup", len(_job_list))
        for _job in _job_list:
            # Check for unit of work time left
            if not self.__check_unit_timer():
                break

            # Do not exceed max thread limit
            #if len(self.__thread_list_cleanup) >= conf.config_max_threads:
            #    break

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
            _job_list = G.STATE_MANAGER.get_job_list(flag=JobState.FLAG_STOP)
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        for _job in _job_list:
            logger.debug('@JManager - Detected job marked for a kill: %s',
                         _job.id())
            logger.log(VERBOSE, 'Job is in "%s" state', _job.get_state())

            # Wait for the job submission thread to finish
            if _job.get_state() == 'processing':
                continue
            # Stop if it is running
            if _job.get_state() == 'running' or \
                    _job.get_state() == 'queued':
                try:
                    G.SCHEDULER_STORE[_job.status.scheduler].stop(
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

        try:
            _job_list = G.STATE_MANAGER.get_job_list(flag=JobState.FLAG_DELETE)
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        for _job in _job_list:
            _jid = _job.id()
            logger.debug('@JManager - Detected job marked for deletion: %s' %
                         _jid)

            # Stop if it is running
            if _job.get_state() in ('running', 'queued'):
                try:
                    G.SCHEDULER_STORE[_job.status.scheduler].stop(
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
                G.STATE_MANAGER.delete_job(_job)
            except:
                #@TODO some limit on remove attempts?
                logger.error("Cannot remove job %s.", _jid, exc_info=True)
                continue

            logger.info('@JManager - Job %s removed with all data.' %
                        _jid)

    def check_old_jobs(self):
        """Check for jobs that exceed their life time.

        If found mark them for removal."""

        logger.log(VERBOSE, '@JManager - Check for expired jobs.')

        try:
            _job_list = G.STATE_MANAGER.get_job_list()
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        for _job in _job_list:
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
            _delete_dt = G.SERVICE_STORE[_job.status.service].config['max_lifetime']
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
                        G.SERVICE_STORE[_job.status.service].config['max_runtime']
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
                try:
                    _job.delete()
                except:
                    logger.error("@JManager - unable schedule job for removal.",
                                 exc_info=True)

    def report_jobs(self):
        """
        Log the current queue state.
        """

        try:
            _results = G.STATE_MANAGER.get_job_state_counters()
        except:
            logger.error("Unable to contact the DB.", exc_info=True)
            return

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
            if _thread.ready():
                try:
                    _thread.get()
                except:
                    logger.error("Subprocess raised an exception.",
                            exc_info=True)
                self.__thread_list_submit.remove(_thread)
                _clean = False
                logger.debug("Removed finished subprocess.")
        for _thread in self.__thread_list_cleanup:
            if not _thread.ready():
                try:
                    _thread.get()
                except:
                    logger.error("Subprocess raised an exception.",
                            exc_info=True)
                self.__thread_list_cleanup.remove(_thread)
                _clean = False
                logger.debug("Removed finished subprocess.")
        if not _clean:
            G.STATE_MANAGER.check_commit()

    def check_stuck_jobs(self):
        """
        Check for jobs in processing and cleanup states. If found when
        AppServer is starting they have to be reset to waiting and closing
        states as otherwise nothing will tuch them. When we are starting no
        worker is present that was supposed to take care of those jobs.
        """
        try:
            _job_list = G.STATE_MANAGER.get_job_list("cleanup")
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        logger.debug("Found %s jobs stuck in closing state", len(_job_list))
        for _job in _job_list:
            _job.close()

        try:
            _job_list = G.STATE_MANAGER.get_job_list("processing")
        except:
            logger.error('Unable to contact with the DB.', exc_info=True)
            return

        logger.debug("Found %s jobs stuck in processing state", len(_job_list))
        for _job in _job_list:
            _job.wait()

        G.STATE_MANAGER.check_commit()

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
            _counters = G.STATE_MANAGER.get_quota_service_counters()
        except:
            logger.error('Unable to contact the DB.', exc_info=True)
            return
        _service_usage = { _key : 0 for _key in G.SERVICE_STORE }
        _service_quota = { _key : _service.config['quota'] \
                for _key, _service in G.SERVICE_STORE.items() }
        for (_count, _key) in _counters:
            _service_usage[_key] = _count

        logger.log(VERBOSE, 'Service quota utilisation: %s.', _service_usage)
        logger.log(VERBOSE, 'Service quota limits: %s.', _service_quota)

        for (_service_name, _service) in G.SERVICE_STORE.items():
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
            _job_list = []
            try:
                _job_list = G.STATE_MANAGER.get_job_list()
            except:
                logger.error('Unable to contact with the DB.', exc_info=True)

            for _job in G.STATE_MANAGER.get_job_list(service=_service_name):
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

        if not G.STATE_MANAGER.check_commit():
            return

        try:
            logger.debug('@JManager - Start submit thread')
            # The sub thread cannot use the same Job instance or the same
            # DB session as the main one. Pass the JobID istead so that the
            # thread can create its own instances.

            _thread = self.__thread_pool_submit.apply_async(
                    func=worker_submit, args=(_job_ids,))
                    #func=worker_submit_profile, args=(_job_ids,))
            self.__thread_list_submit.append(_thread)
            logger.debug("@JManager - Submit thread started.")
        except:
            logger.error("@JManager - Unable to start submit thread %s",
                         exc_info=True)

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
        if not G.STATE_MANAGER.check_commit():
            return

        try:
            logger.debug('Start cleanup thread')
            # The sub thread cannot use the same Job instance or the same
            # DB session as the main one. Pass the JobID istead so that the
            # thread can create its own instances.

            _thread = self.__thread_pool_cleanup.apply_async(
                    func=worker_cleanup, args=(_job_ids,))
                    #func=worker_cleanup_profile, args=(_job_ids,))
            self.__thread_list_cleanup.append(_thread)
            logger.debug("@JManager - Cleanup thread %s started.")
        except:
            logger.error("@JManager - Unable to start cleanup thread %s",
                         exc_info=True)

    def shutdown(self):
        """
        Stop all running jobs.
        """
        logger.info("Starting shutdown")

        # Push to DB not commited changes and expire local cache
        G.STATE_MANAGER.check_commit()

        time.sleep(conf.config_shutdown_time)
        self.check_running_jobs()
        self.check_cleanup()

        # Kill running, queued and waiting jobs
        if self.__terminate:
            _job_list = []
            try:
                _job_list = G.STATE_MANAGER.get_job_list()
            except:
                logger.error('Unable to contact with the DB.', exc_info=True)

            for _job in _job_list:
                _state = _job.get_state()
                if _state in ('queued', 'running'):
                    _scheduler = G.SCHEDULER_STORE[_job.status.scheduler]
                    try:
                        _scheduler.stop(_job, 'Server shutdown', ExitCodes.Shutdown)
                    except CisError as e: # Temporary job stop problem
                        continue
                    except:
                        _job.die("@PBS - Unable to terminate job %s." %
                                _job.id(), exc_info=True)
                elif _state == 'waiting':
                    _job.finish('User request', 'killed', ExitCodes.Shutdown)
                #@TODO What with the processiong jobs
            # Push to DB not commited changes and expire local cache
            G.STATE_MANAGER.check_commit()
            # Wait for PBS to kill the jobs
            time.sleep(conf.config_shutdown_time)

        # Check for jobs that got killed and lunch cleanup
        self.check_running_jobs()
        self.check_cleanup()
        time.sleep(conf.config_shutdown_time)

        self.clear()

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
        _n = 0

        # Recover jobs stuck in processing and cleanup states
        self.check_stuck_jobs()

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
            logger.debug("Iteration time: %s", _exec_time)
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
            try:
                G.STATE_MANAGER.poll_gw()
            except:
                logger.error("Unable to poll GW.",
                             exc_info=True)
            self.check_running_jobs()
            if self.__queue_running:  # Do not process queue in pause state
                self.check_new_jobs()
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
            # Commit changes to the DB. This should expire local cache and resync it with DB on next access.
            if not G.STATE_MANAGER.check_commit():
                time.sleep(5)
                if not G.STATE_MANAGER.check_commit():
                    self.shutdown()
            _n += 1

        logger.debug("Main Loop End")

        self.shutdown()

    def __check_unit_timer(self):
        _exec_time = (datetime.utcnow() - self.__time_stamp_unit).total_seconds()
        return (_exec_time < (2.0 * conf.config_sleep_time / 3.0))

    def __start_unit_timer(self):
        self.__time_stamp_unit = datetime.utcnow()


def worker_init(config, work_id, lock):
    """
    Initialize worker process.

    :param config: Config.Config instance.
    """
    threading.current_thread().name = "%s_%s" % (work_id, os.getpid())
    # Initialize config
    conf.update(config)
    # Initialize logging
    logging.config.dictConfig(conf.log_config)
    # Disable console logging
    _h = logging.root.handlers[0]
    logging.root.removeHandler(_h)
    # Initialize DB - has to be serialized
    lock.acquire()
    G.init()
    lock.release()

def worker_submit_profile(job_ids):
    """
    Profile submit worker.
    """
    cProfile.runctx('worker_submit(job_ids)', globals(), locals(),
                    'submit_prof%d.prof' % os.getpid())

def worker_submit(job_ids):
    """
    Generate job related scripts and submit them to selected scheduler.

    :param job: The Job object to submit.
    :return: True on success, False otherwise.
    """
    logger.debug("Submit batch of %s jobs.", len(job_ids))

    try:
        _session = G.STATE_MANAGER.new_session(autoflush=False)
        # Do an eager load so that no more SELECT statements are issued.
        # This should prevent issuing update statements on job changes
        # untli commit is issued and will prevent the DB lock.
        _jobs = G.STATE_MANAGER.get_job_list_byid(job_ids, session=_session, full=True)
    except:
        logger.error("Unable to connect to DB.",
                     exc_info=True)
        #@TODO we should somehow recover from this otherwise jobs will remain in processing state forever
        return

    for _job in _jobs:
        _jid = _job.id()

        # Validate input
        try:
            G.VALIDATOR.validate(_job)
        except ValidatorInputFileError as e:
            # The input file is not available yet. Flag the job to wait.
            _job.set_flag(JobState.FLAG_WAIT_INPUT)
            _job.status.wait_time = datetime.utcnow()
            _job.wait()
            logger.log(VERBOSE, 'Job flagged to wait for input.')
            continue
        except ValidatorError as e:
            # Error in job input detected log a warning
            if logger.getEffectiveLevel() > logging.DEBUG:
                _job.die("@worker_submit - Job validation failed: %s" % e.message,
                         err=False, exit_code=ExitCodes.Validate)
            else:
                _job.die("@worker_submit - Job validation failed: %s" % e.message,
                         exc_info=True, err=False, exit_code=ExitCodes.Validate)
            continue
        except:
            # Unhandled exception log an error
            _job.die("@worker_submit - Job validation failed.",
                     exc_info=True, exit_code=ExitCodes.Validate)
            continue

        # During validation default values are set in Job.valid_data
        # Now we can access scheduler selected for current Job
        try:
            _scheduler = G.SCHEDULER_STORE[_job.status.scheduler]
        except:
            _job.die("Unable to obtain scheduler and "
                         "service instance.", exc_info=True)
            continue
        # Ask scheduler to generate scripts and submit the job
        try:
            if _scheduler.generate_scripts(_job):
                if _scheduler.chain_input_data(_job):
                    if _scheduler.submit(_job):
                        _job.queue()
                    else:
                        _job.wait()
        except:
            _job.die("Unable to submit job.", exc_info=True)
            continue

    G.STATE_MANAGER.check_commit(_session)
    logger.debug("Job submit thread finished.")

def worker_cleanup_profile(job_ids):
    """
    Profile cleanup worker.
    """
    cProfile.runctx('worker_cleanup(job_ids)', globals(), locals(),
                    'cleanup_prof%d.prof' % os.getpid())

def worker_cleanup(job_ids):
    """
    """
    logger.debug("Cleanup batch of %s jobs.", len(job_ids))

    try:
        _session = G.STATE_MANAGER.new_session(autoflush=False)
        _jobs = G.STATE_MANAGER.get_job_list_byid(job_ids, session=_session, full=True)
    except:
        logger.error("Unable to connect to DB.",
                     exc_info=True)
        return

    for _job in _jobs:
        _jid = _job.id()

        # Jobs killed in waiting state will not have a scheduler defined. There
        # is no cleanup to perform either. Simply call exit ...
        if _job.status.scheduler is None:
            _job.exit()
        else:
            try:
                _scheduler = G.SCHEDULER_STORE[_job.status.scheduler]
                if _job.get_exit_state() == 'aborted':
                    _scheduler.abort(_job)
                else:
                    _scheduler.finalise(_job)
            except:
                #TODO mark job as aborted?
                logger.error("Job %s cleanup finished with error.",
                             _jid, exc_info=True)
                continue

    G.STATE_MANAGER.check_commit(_session)
    logger.debug("Job cleanup thread finished.")

