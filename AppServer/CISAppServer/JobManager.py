#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
#import sys
import argparse
import logging
try:
    import json
except:
    import simplejson as json
import shutil
import time

from logging import \
    debug, info, warning, error

import Tools as T
from Config import conf

version = "0.2"


class Job(object):
    """
    Class that implements a job instance.

    Allows to:

    * query/set job state,
    * generate the output status file,
    * query job parameters defined in job request.
    """

    def __init__(self, job_id):
        """
        Works with existing job requests that are identified by their unique ID
        string. Upon initialization loads job parameters from job request JSON
        file.

        :param job_id: The unique job ID.
        """
        #: Unique job ID string.
        self.id = job_id
        #: Job parameters as specified in job request.
        self.data = {}
        #: Job parameters after validation
        self.valid_data = {}
        self.__state = None

        # Load job file from jobs directory
        try:
            _name = os.path.join(conf.gate_path_jobs, self.id)
            with open(_name) as _f:
                self.data = json.load(_f)
            debug('@Job - Loaded data file %s.' % self.id)
            debug(self.data)
        except:
            self.die("@Job - Cannot load data file %s." % _name, exc_info=True)
            return None

        try:
            self.__check_state()
        except:
            self.die("@Job - Unable to check state (%s)." % self.id,
                     exc_info=True)
            return None

    def get_state(self):
        """
        Get current job state.

        :return: One of valid job states:

        * waiting:
            job request was submitted and is waiting for JobManager to process
            it,
        * queued:
            job reuest was processed and is queued in the scheduling backend,
        * running:
            job is running on a compute node,
        * done:
            job has finished,
        * aborted:
            an error occurred during job preprocessing, submission or
            postprocessing,
        * killed:
            job execution was stopped by an user,
        * removed:
            job is scheduled for removal.
        """

        if os.path.exists(os.path.join(conf.gate_path_removed, self.id)):
            self.__state = 'removed'

        return self.__state

    def set_state(self, new_state):
        """
        Change job state.

        :param new_state: new sate for the job. For valid states see
            :py:meth:`get_state`.
        """
        debug("@Job - Set new state: %s" % new_state)

        if new_state not in conf.service_states:
            raise Exception('Unknown job state %s.' % new_state)

        self.get_state()
        if self.__state == 'removed':
            return

        if self.__state == new_state:
            return

        # Mark new state in the shared file system
        os.symlink(
            os.path.join(conf.gate_path_jobs, self.id),
            os.path.join(conf.gate_path[new_state], self.id)
        )

        # Remove all other possible states just in case we previously failed
        for _state in conf.service_states:
            if _state != new_state and _state != 'removed':
                _name = os.path.join(conf.gate_path[_state], self.id)
                if os.path.exists(_name):
                    os.unlink(_name)

        self.__state = new_state

    def die(self, message, err=True, exc_info=False):
        """
        Abort further job execution with proper error in AppServer log as well
        as with proper message for clinet in the job output status file. Sets
        job state as *aborted*.

        :param message: Message to be passed to logs and client,
        :param err: if True use log as ERROR otherwise use WARNING priority,
        :param exc_info: if True logging will extract current exception info
            (use in except block to provide additional information to the
            logger).
        """

        self.exit(message, 'aborted')
        if err:
            error(message, exc_info=exc_info)
        else:
            warning(message, exc_info=exc_info)

    def exit(self, message, state='done'):
        """
        Set one of job finished states and generate job output status file.

        The output file is created on the shared storage so that the AppGateway
        can pass the job status and the message to clients.

        :param message: Message that will be written to the output status file.
        :param state: Job state to set. Valid values are 'done', aborted',
            'killed'
        """

        # Prefixes for output status messages
        _states = {
            'done': 'Done: ',
            'failed': 'Failed: ',
            'aborted': 'Aborted: ',
            'killed': 'Killed: '
        }

        # Only three output states are allowed
        if state not in _states.keys():
            _msg = "@Job - Wrong job exit status provided: %s." % state
            message = _msg + 'Original message:\n' + message
            state = 'aborted'

        # Prepend the state prefix to status message
        message = _states[state] + message
        _name = os.path.join(conf.gate_path_exit, self.id)
        # Generate the output status file
        try:
            self.set_state(state)
            # Use append in case we got called several times due to consecutive
            # errors
            with open(_name, 'a') as _f:
                _f.write(message)
        except:
            error("@Job - Cannot write to status file %s." % _name,
                  exc_info=True)

        info("Job %s finished: %s" % (self.id, state)

    def __check_state(self):
        """
        Query the shared storage to identify current job state.

        In normal situation job remembers its state internally. However when
        restarting AppServer after shutdown with already running jobs this will
        allow to reload them.
        """
        if self.__state == 'removed':
            return

        if os.path.exists(os.path.join(conf.gate_path_removed, self.id)):
            self.__state = 'removed'
        elif os.path.exists(os.path.join(conf.gate_path_aborted, self.id)):
            self.__state = 'aborted'
        elif os.path.exists(os.path.join(conf.gate_path_killed, self.id)):
            self.__state = 'killed'
        elif os.path.exists(os.path.join(conf.gate_path_failed, self.id)):
            self.__state = 'failed'
        elif os.path.exists(os.path.join(conf.gate_path_done, self.id)):
            self.__state = 'done'
        elif os.path.exists(os.path.join(conf.gate_path_running, self.id)):
            self.__state = 'running'
        elif os.path.exists(os.path.join(conf.gate_path_queued, self.id)):
            self.__state = 'queued'
        elif os.path.exists(os.path.join(conf.gate_path_waiting, self.id)):
            self.__state = 'waiting'


class JobManager(object):
    """
    Main calss of CISAppServer. It is responsible for job management.
    """

    #TODO daemon mode
    #TODO on the fly configuration reload
    #TODO queue stop (do not accept new jobs)

    def __init__(self):
        """
        Upon initialization parses command line arguments, loads options
        from input files. Initializes Validator and Sheduler interfaces.
        """

        # Options parser
        _desc = 'Daemon responsible for handling CIS Web Apps Requests.'
        _parser = argparse.ArgumentParser(description=_desc)
        _parser.add_argument(
            '-c', '--config', dest='config', action='store',
            default='CISAppServer.json', help='Configuration file.')
        _parser.add_argument(
            '--log', dest='log', action='store', default='DEBUG',
            help='Logging level: VERBOSE, DEBUG, INFO, WARNING, ERROR.')
        _parser.add_argument(
            '--log-output', dest='log_output', action='store',
            help='Store the logs into LOG-OUTPUT.')
        _args = _parser.parse_args()

        # Setup logging interface
        conf.log_level = _args.log.upper()  #: Logging level to use
        conf.log_output = _args.log_output  #: Log output file name
        logging.addLevelName(T.VERBOSE, 'VERBOSE')
        _log_level = getattr(logging, conf.log_level)
        if conf.log_output:
            logging.basicConfig(level=_log_level, filename=conf.log_output)
        else:
            logging.basicConfig(level=_log_level)

        info("CISAppS %s" % version)
        info("Logging level: %s" % conf.log_level)
        info("Configuration file: %s" % _args.config)

        # Load configuration from option file
        debug('@JManager - Loading global configuration ...')
        conf.load(_args.config)

        # Initialize Validator and PbsManager
        self.validator = T.Validator()  #: Validator instance
        self.schedulers = {  #: Scheduler interface instances
            'pbs': T.PbsScheduler()
        }

        # Job list
        self.__jobs = {}

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
        if job_id not in self.__jobs.keys():
            error('@JManager - Job %s is missing from '
                  'overall job list.' % job_id)
            if create:
                _job = Job(job_id)
                self.__jobs[job_id] = _job
                return _job
            else:
                return None

        return self.__jobs[job_id]

    def check_new_jobs(self):
        """
        Check for new job files in the queue directory.

        If found try to submit them to selected job scheduler.
        """

        # New jobs are put into the "waiting" directory
        try:
            _queue = os.listdir(conf.gate_path_waiting)
        except:
            error("@JManager - Unable to read waiting queue %s" %
                  conf.gate_path_waiting, exc_info=True)
            return

        for _jid in _queue:
            debug('@JManager - Detected new job %s.' % _jid)

            # Create ne job instance
            _job = Job(_jid)
            if _job is None:
                continue

            if _jid in self.__jobs.keys():
                _job.die("@JManager - Job ID already used: %s" % _jid)
                continue

            try:
                self.__jobs[_jid] = _job
                if self.submit(_job):
                    _job.set_state('queued')
                else:
                    # Assume that the error was during parsing of input data.
                    # If it was otherwise lower level routines should have
                    # issued and ERROR already.
                    _job.die("@JManager - Cannot start job %s." % _jid,
                             err=False)
            except:
                _job.die("@JManager - Cannot start job %s." % _jid,
                         exc_info=True)

    def check_running_jobs(self):
        """
        Check status of running jobs.

        If finished jobs are found perform finalisation.
        """

        # Loop over supported schedulers
        for _sname, _scheduler in self.schedulers.items():
            # Check the "queue_path" for files with scheduler IDs. This
            # directory should only be accessible inside of the firewall so it
            # should be safe from corruption by clients.
            try:
                _queue = os.listdir(_scheduler.queue_path)
            except:
                error("@JManager - Unable to read %s queue directory %s." %
                      (_sname, _scheduler.queue_path), exc_info=True)
                continue

            for _jid in _queue:
                # Get Job object
                _job = self.get_job(_jid)
                # Handle zombies
                if _job is None:
                    error('@JManager - Job %s in %s queue is missing from '
                          'overall job list.' % (_jid, _sname))
                    try:
                        os.unlink(os.path.join(_scheduler.queue_path, _jid))
                    except:
                        error('@JManager - Unable to remove dangling job ID '
                              '%s from %s queue.' % (_jid, _sname))
                    continue

                # Get Job status
                _status = _scheduler.status(_job)
                if _status == 'done':
                    # Found finished job - finalise it
                    debug('@JManager - Detected finished job: %s.' % _jid)
                    if not _scheduler.finalise(_job):
                        _job.die("Cannot finalise job %s." % _jid)
                elif _status == 'unknown':
                    warning(
                        "@JManager - Scheduler returned 'unknown' status "
                        "for job %s" % _jid
                    )

    def check_deleted_jobs(self):
        """
        Check for jobs marked for removal.

        If found remove all resources related to the job. If a job is still
        running kill it.
        """

        # Symlinks in "delete" dir mark jobs for removal
        try:
            _queue = os.listdir(conf.gate_path_removed)
        except:
            error("@JManager - Unable to read delete queue: %s." %
                  conf.gate_path_removed, exc_info=True)
            return

        for _jid in _queue:
            debug('@JManager - Detected job marked for deletion: %s' % _jid)

            _job = self.get_job(_jid, create=True)
            if _job is None:
                continue

            try:
                # Stop if it is running
                if _job.get_state() == 'running':
                    self.schedulers[_job.valid_data['scheduler']].stop(_job)
                # Remove job file and its symlinks
                os.unlink(os.path.join(conf.gate.path_jobs, _jid))
                for _state, _path in conf.gate_path:
                    _name = os.path.join(_path, _jid)
                    if os.path.exists(_name):
                        os.unlink(_name)
                # Remove output status file
                _name = os.path.join(conf.gate_path_exit, _jid)
                if os.path.exists(_name):
                    os.unlink(_name)
            except:
                error("Cannot remove job %s." % _jid, exc_info=True)

            # Remove the output directory and its contents
            _output = os.path.join(conf.gate_path_output, _jid)
            if os.path.isdir(_output):
                shutil.rmtree(_output, onerror=T.rmtree_error)

            # Remove job from the list
            del self.__jobs[_jid]

    def check_old_jobs(self):
        """Check for jobs that exceed their life time.

        If found mark them for removal. [Not Implemented]"""
        pass

    def submit(self, job):
        """
        Generate job related scripts and submit them to selected scheduler.

        :param job: The Job object to submit.
        :return: True on success, False otherwise.
        """

        if self.validator.validate(job):  # Validate input
            # During validation default values are set in Job.valid_data
            # Now we can access scheduler selected for current Job
            _scheduler = self.schedulers[job.valid_data['scheduler']]
            # Ask scheduler to generate scripts and submit the job
            if _scheduler.generate_scripts(job):
                return _scheduler.submit(job)

        return False

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
            self.check_new_jobs()
            self.check_running_jobs()
            self.check_old_jobs()
            self.check_deleted_jobs()


if __name__ == "__main__":
    apps = JobManager()
    apps.run()
