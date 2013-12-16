#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""

"""

import os
import json
import logging
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime

from Config import conf, verbose, ExitCodes
from DataStore import JobStore, SchedulerStore


logger = logging.getLogger(__name__)


class JobBadState(Exception):
    """ Wrong job state exception. """


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
        #: Jobs' Service
        self.service = None
        #: Job parameters as specified in job request.
        self.data = {}
        #: Job parameters after validation
        self.valid_data = {}
        #: List of job IDs whose output we would like to consume (job chaining)
        self.chain = []
        self.__state = None  # Current job state
        self.__exit_message = ""  # Job exit message
        self.__exit_state = None  # Job exit state
        self.__exit_code = ExitCodes.Undefined  # Job exit code
        self.__size = 0  # Job output size in bytes

        # Load job file from jobs directory
        try:
            _name = os.path.join(conf.gate_path_jobs, self.id)
            with open(_name) as _f:
                self.data = json.load(_f)
            logger.debug(u'@Job - Loaded data file %s.' % self.id)
            logger.debug(self.data)
        except:
            self.die(u"@Job - Cannot load data file %s." % _name,
                     exc_info=True)
            return None

        # Load job state
        try:
            self.__state = StateManager.get_state(self.id)
        except:
            self.die(u"@Job - Unable to check state (%s)." % self.id,
                     exc_info=True)
            return None

        # Load job internal data
        try:
            _data = StateManager.get_data(self.id)
            if "exit_state" in _data.keys():
                self.__exit_state = _data["exit_state"]
            if "exit_code" in _data.keys():
                self.__exit_code = _data["exit_code"]
            if "exit_message" in _data.keys():
                self.__exit_message = _data["exit_message"]
        except:
            logger.error("@Job - Unable to load job info from: %s." %
                         _name, exc_info=True)

        # Check if the job has defined submit time. If yes this means it was
        # submitted by previous JobManager instance.
        _time = None
        try:
            _time = StateManager.get_time(self.id, 'submit')
        except JobBadState:
            pass
        except:
            self.die(u"@Job - Unable to check submit time (%s)." % self.id,
                     exc_info=True)
            return None

        # Set job submit time if required
        if _time is None:
            try:
                StateManager.set_time(self.id, 'submit')
            except:
                self.die(u"@Job - Unable to set submit time (%s)." % self.id,
                         exc_info=True)
                return None

    def get_size(self):
        """
        Get the size of job output directory.
        Requires that calculate_size was called first.

        :return: Size of output directory in bytes.
        """
        return self.__size

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
        * closing:
            job has finished and is waiting for cleanup,
        * cleanup:
            job has finished and cleanup of resources is performed,
        * done:
            job has finished,
        * failed:
            job has finished with non zero exit code,
        * aborted:
            an error occurred during job preprocessing, submission or
            postprocessing,
        * killed:
            job was killed,
        """

        return self.__state

    def get_exit_state(self):
        """
        Get job "exit state" - the state that job will be put into after it is
        finalised. The "exit state" is set using :py:meth:`finish`.

        :return: One of valid job exit states or None if the "exit state" is
        not yet defined:

        * done:
            job has finished,
        * failed:
            job has finished with non zero exit code,
        * aborted:
            an error occurred during job preprocessing, submission or
            postprocessing,
        * killed:
            job was killed,
        """

        return self.__exit_state

    def queue(self):
        """ Mark job as queued. """
        self.__set_state('queued')

    def run(self):
        """ Mark job as running. """
        self.__set_state('running')
        StateManager.set_time(self.id, 'start')

    def cleanup(self):
        """ Mark job as in cleanup state. """
        self.__set_state('cleanup')

    def delete(self):
        """ Mark job for removal. """
        StateManager.set_flag(self.id, "delete")

    def mark(self, message, exit_code=ExitCodes.UserKill):
        """
        Mark job as killed by user.

        :param message: that will be passed to user,
        :param exit_code: one of :py:class:`ExitCodes`.
        """
        # Mark as killed makes sense only for unfinished jobs
        if self.get_state() not in ['waiting', 'queued', 'running']:
            logger.warning("@Job - Job %s already finished, cannot mark as "
                           "killed" % self.id)
            return

        self.__set_exit_state(message, 'killed', exit_code)

    def finish(self, message, state='done', exit_code=0):
        """
        Mark job as finished. Job will be set into *closing* state. When
        cleanup is finished the :py:meth:`exit` method should be called to set
        the job into its "exit state".

        :param message: that will be passed to user,
        :param state: Job state after cleanup will finish. One of:
            ['done', 'failed', 'aborted', 'killed'],
            :param exit_code: one of :py:class:`ExitCodes`.
        """
        self.__set_exit_state(message, state, exit_code)
        self.__set_state('closing')

    def die(self, message, exit_code=ExitCodes.Abort,
            err=True, exc_info=False):
        """
        Abort further job execution with proper error in AppServer log as well
        as with proper message for client in the job output status file. Sets
        job state as *closing*. After cleanup the job state will be *aborted*.

        :param message: Message to be passed to logs and client,
        :param exit_code: One of :py:class:`ExitCodes`,
        :param err: if True use log as ERROR otherwise use WARNING priority,
        :param exc_info: if True logging will extract current exception info
            (use in except block to provide additional information to the
            logger).
        """

        if err:
            logger.error(message, exc_info=exc_info)
        else:
            logger.warning(message, exc_info=exc_info)

        try:
            self.finish(message, 'aborted', exit_code)
        except:
            logger.error("@Job - Unable to mark job %s for finalise step." %
                         self.id, exc_info=True)
            self.__state = 'aborted'

    def exit(self):
        """
        Finalise job cleanup by setting the state and passing the exit message
        to the user. Should be called only after Job.finish() method was
        called.
        """

        if self.__exit_state is None:
            self.die("@Job - Exit status is not defined for job %s." %
                     self.id)
            return

        _name = os.path.join(conf.gate_path_exit, self.id)
        # Generate the output status file
        try:
            self.__set_state(self.__exit_state)
            with open(_name, 'w') as _f:
                _f.write(self.__exit_message)
        except:
            logger.error("@Job - Cannot write to status file %s." % _name,
                         exc_info=True)

        # Store stop time
        try:
            StateManager.set_time(self.id, 'stop')
        except:
            logger.error("@Job - Cannot store job stop time.", exc_info=True)

        logger.info("Job %s finished: %s" % (self.id, self.__exit_state))

    def calculate_size(self):
        """
        Calculate size of output directory of the job if it exists.
        """
        self.__size = 0

        # Only consider job states that could have an output dir
        if self.__state not in \
           ('cleanup', 'done', 'failed', 'killed', 'abort'):
            return

        try:
            # Check that output dir exists
            _name = os.path.join(conf.gate_path_output, self.id)
            if not os.path.exists(_name):
                self.__size = 0
                verbose("@Job - Job output size calculated: %s" %
                        self.__size)
                return

            # Use /usr/bin/du as os.walk is very slow
            _opts = [u'/usr/bin/du', u'-sb', _name]
            verbose("@Job - Running command: %s" % str(_opts))
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()
            verbose(_output)
            # Hopefully du returned meaningful result
            _size = _output[0].split()[0]
            # Check return code. If du was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/du returned non zero exit code.\n%s" %
                    str(_output)
                ))
        except:
            logger.error(
                "@Job - Unable to calculate job output size %s." % self.id,
                exc_info=True
            )
            return

        self.__size = int(_size)
        verbose("@Job - Job output size calculated: %s" % self.__size)

    def compact(self):
        """
        Release resources allocated for the job (data, valid_data).
        """
        self.data = {}
        self.valid_data = {}

    def __set_state(self, new_state):
        """
        Change job state.

        :param new_state: new sate for the job. For valid states see
            :py:meth:`get_state`.
        """
        if new_state not in conf.service_states:
            raise Exception("Unknown job state %s (%s)." %
                            (new_state, self.id))

        StateManager.set_state(self.id, new_state)
        self.__state = new_state

    def __set_exit_state(self, message, state, exit_code):
        """
        Set job "exit state" - the state that job will be put into after it is
        finalised.

        :param message: - Message that will be passed to the user,
        :param state: - Job state, one of: done, failed, aborted, killed,
        :param exit_code: - One of :py:class:`ExitCodes`.
        """
        # Valid output states
        _states = ('done', 'failed', 'aborted', 'killed')

        if state not in _states:
            raise Exception("Wrong job exit state: %s." % state)

        # Prepend the state prefix to status message
        _prefix = state[:1].upper() + state[1:]
        _message = "%s:%s %s\n" % \
            (_prefix, exit_code, message)

        # Do not overwrite aborted or killed states
        if self.__exit_state != 'aborted':
            if self.__exit_state != 'killed' or state == 'aborted':
                self.__exit_state = state
                self.__exit_code = exit_code
                # Concatanate all status messages
                self.__exit_message += _message

                # Store the exit info
                try:
                    _data = StateManager.get_data(self.id)
                    _data["exit_state"] = self.__exit_state,
                    _data["exit_code"] = self.__exit_code,
                    _data["exit_message"] = self.__exit_message
                    StateManager.set_data(self.id, _data)
                except:
                    if self.__exit_state != 'aborted':
                        raise
                    else:
                        logger.error('@Job - Unable to store job internal'
                                     'state', exc_info=True)


class StateManager(object):
    """
    Interface for persistent storage of job states.

    Default implementation uses files on shared file system.
    """
    def get_job(self, job_id, create=False, sloppy=False):
        """
        Get Job object from JobStore.

        :param job_id: Job unique ID,
        :param create: If job identified by *job_id* is not found in the
            JobStore an ERROR message is logged. When create is True a new Job
            object is created and added to the JobStore,
        :param sloppy: If job identified by *job_id* is not found in the
            JobStore an ERROR message is logged. When sloppy is True the ERROR
            message is suppressed (useful to skip jobs that are still waiting
            for validation).
        :return: Job object if *job_id* was found, None otherwise.
        """

        # Handle zombies
        if job_id not in JobStore.keys():
            if create:
                _job = Job(job_id)
                JobStore[job_id] = _job
                return _job
            elif not sloppy:
                logger.error('@StateManager - Job %s is missing from '
                             'overall job list.' % job_id)
            return None

        return JobStore[job_id]

    def get_job_list(self, state="all", create=False, sloppy=False):
        """
        Get a dictionary of jobs that are in a selected state.

        :param state: Specifies state for which Jobs will be selected. For
            valid states see :py:meth:`get_state`. To select all jobs specify
            'all' as the state.
        :param create: For jobs that do not have a matching Job instance
            an ERROR message is logged. When create is True a new Job object is
            created and added to the JobStore,
        :param sloppy: For jobs that do not have a matching Job instance
            an ERROR message is logged. When sloppy is True the ERROR
            message is suppressed and None is set instead of Job instance
            (useful to skip jobs that are still waiting for validation).

        :return: Dictionary with job_id as key and Job instance as value.
        """
        _states = ("waiting", "queued", "running", "closing", "cleanup",
                   "done", "failed", "aborted", "killed", "delete", "stop")
        if state != 'all' and state not in _states:
            logger.error("@StateManager - Unknown state: %s" % state)
            return

        _path = conf.gate_path_jobs
        if state != 'all':
            _path = conf.gate_path[state]

        _jobs = {}
        try:
            _list = os.listdir(_path)
        except:
            logger.error(u"@JManager - Unable to read directory: %s." %
                         _path, exc_info=True)
            return

        for _jid in _list:
            _job = self.get_job(_jid, create=create, sloppy=sloppy)
            if _job is None:
                continue
            _jobs[_jid] = _job

        return _jobs

    def get_scheduler_list(self, scheduler):
        """
        Get a dictionary of jobs that are in a scheduler queue.

        :param scheduler: Specifies scheduler name for which Jobs will be
            selected.

        :return: Dictionary with scheduler_job_id as key and Job instance as
            value.
        """
        _job_list = {}

        try:
            _scheduler = SchedulerStore[scheduler]
        except:
            logger.error(
                "@StateManager - unknown scheduler %s." %
                scheduler, exc_info=True
            )
            return _job_list

        # check the "queue_path" for files with scheduler ids. this
        # directory should only be accessible inside of the firewall so it
        # should be safe from corruption by clients.
        try:
            _queue = os.listdir(_scheduler.queue_path)
        except:
            logger.error(
                "@StateManager - unable to read %s queue directory %s." %
                (scheduler, _scheduler.queue_path),
                exc_info=True
            )
            return _job_list

        for _jid in _queue:
            # Get Job object
            _job = JobStore[_jid]
            # Handle zombies
            if _job is None:
                logger.error(
                    '@StateManager - Job %s in %s queue is missing from '
                    'overall job list.' % (_jid, scheduler)
                )
                try:
                    os.unlink(os.path.join(_scheduler.queue_path, _jid))
                except:
                    logger.error(
                        '@StateManager - Unable to remove dangling job ID '
                        '%s from %s queue.' % (_jid, scheduler)
                    )
                continue

            try:
                with open(os.path.join(_scheduler.queue_path, _jid)) as \
                        _job_file:
                    _sid = _job_file.readline().strip()
            except:
                _job.die("@StateManager - Unable to read % job ID" % scheduler,
                         exc_info=True)
                continue

            _job_list[_sid] = _job

        return _job_list

    def get_scheduler_count(self, scheduler):
        """
        Get a count jobs that are in a scheduler queue.

        :param scheduler: Specifies scheduler name for which Jobs will be
            selected.

        :return: Number of jobs in the scheduler queue, or -1 in case of error.
        """
        _job_count = -1

        try:
            _scheduler = SchedulerStore[scheduler]
        except:
            logger.error(
                "@StateManager - unknown scheduler %s." %
                scheduler, exc_info=True
            )
            return _job_count

        # check the "queue_path" for files with scheduler ids. this
        # directory should only be accessible inside of the firewall so it
        # should be safe from corruption by clients.
        try:
            _queue = os.listdir(_scheduler.queue_path)
        except:
            logger.error(
                "@StateManager - unable to read %s queue directory %s." %
                (scheduler, _scheduler.queue_path),
                exc_info=True
            )
            return _job_count

        _job_count = len(_queue)
        return _job_count

    def get_state(self, jid):
        """
        Query the persistent storage to identify current job state.

        In normal situation job remembers its state internally. However when
        restarting AppServer after shutdown with already running jobs this will
        allow to reload them.
        """

        if os.path.exists(os.path.join(conf.gate_path_aborted, jid)):
            return 'aborted'
        elif os.path.exists(os.path.join(conf.gate_path_killed, jid)):
            return 'killed'
        elif os.path.exists(os.path.join(conf.gate_path_failed, jid)):
            return 'failed'
        elif os.path.exists(os.path.join(conf.gate_path_done, jid)):
            return 'done'
        elif os.path.exists(os.path.join(conf.gate_path_cleanup, jid)):
            return 'cleanup'
        elif os.path.exists(os.path.join(conf.gate_path_closing, jid)):
            return 'closing'
        elif os.path.exists(os.path.join(conf.gate_path_running, jid)):
            return 'running'
        elif os.path.exists(os.path.join(conf.gate_path_queued, jid)):
            return 'queued'
        elif os.path.exists(os.path.join(conf.gate_path_waiting, jid)):
            return 'waiting'

    def set_state(self, jid, new_state):
        """
        Change job persistent state.

        :param new_state: new sate for the job. For valid states see
            :py:meth:`get_state`.
        :throws:
        """
        logger.debug("@StateManager - Set new state: %s (%s)" %
                     (new_state, jid))

        job = self.get_job(jid)

        if new_state not in conf.service_states:
            raise Exception("Unknown job state %s (%s)." %
                            (new_state, jid))

        _state = job.get_state()

        if _state == new_state:
            return

        # Mark new state in the shared file system
        os.symlink(
            os.path.join(conf.gate_path_jobs, jid),
            os.path.join(conf.gate_path[new_state], jid)
        )

        # Remove all other possible states just in case we previously failed
        for _state in conf.service_states:
            if _state != new_state:
                _name = os.path.join(conf.gate_path[_state], jid)
                if os.path.exists(_name):
                    os.unlink(_name)

    def get_scheduler_id(self, jid, scheduler):
        """
        Get the ID assigned to the job by the scheduler.

        :param jid: Job ID,
        :param scheduler: Name of the scheduler controling the job.
        :return: Scheduler job ID.
        :throws:
        """
        _scheduler = SchedulerStore[scheduler]
        with open(os.path.join(_scheduler.queue_path, jid)) as _sid_file:
            _sid = _sid_file.readline().strip()
        return _sid

    def set_scheduler_id(self, jid, scheduler, sid):
        """
        Set the ID assigned to the job by the scheduler.

        :param jid: Job ID,
        :param scheduler: Name of the scheduler controling the job.
        :param sid: Scheduler job ID.
        :throws:
        """
        _scheduler = SchedulerStore[scheduler]
        _fname = os.path.join(_scheduler.queue_path, jid)
        if sid is not None:
            with open(_fname, 'w') as _sid_file:
                _sid_file.write(sid)
        else:
            os.unlink(_fname)

    def get_time(self, jid, event="start"):
        """
        Get the time of job submit, start and stop events.

        :param jid: Job ID,
        :param event: Type of time event: submit, start, stop,
        :return: datetime instance with time of the event,
        :throw JobBadState: exception is raised when event time stamp is
          missing, it is up to the caller to decide if this is reasonable or an
          error,
        :throw:
        """
        if event not in ('start', 'stop', 'submit'):
            raise Exception("Unknown time event: %s." % event)

        _path = os.path.join(conf.gate_path_time, event + '_' + jid)
        if not os.path.exists(_path):
            if event == 'start':
                raise JobBadState("Job %s not started yet." % jid)
            elif event == 'stop':
                raise JobBadState("Job %s not finished yet." % jid)
            else:
                raise JobBadState("Job %s timestamp missing." % jid)
        _path_time = datetime.fromtimestamp(os.path.getctime(_path))
        return _path_time

    def set_time(self, jid, event="start", time_stamp=None):
        """
        Set the time of job submit, start and stop events.

        :param jid: Job ID,
        :param event: Type of time event: submit, start, stop,
        :param time_stamp: datetime instance with time of the event. If set to
          None use current time,
        :throw:
        """
        if event not in ('start', 'stop', 'submit'):
            raise Exception("Unknown time state: %s." % event)

        _fname = os.path.join(conf.gate_path_time, event + "_" + jid)
        with file(_fname, 'a'):
            os.utime(_fname, time_stamp)

    def get_data(self, jid):
        """
        Get job persistent data as dictionary of key:value pairs.

        :param jid: Job Id,
        :return: dictionary with job internal data e.g. exit_state
        """
        _name = os.path.join(conf.gate_path_opts, jid)
        _data = {}
        if os.path.isfile(_name):
            with open(_name) as _f:
                _data = json.load(_f)
        return _data

    def set_data(self, jid, data):
        _allowed = ("exit_state", "exit_code", "exit_message")
        for _key in data.keys():
            if _key not in _allowed:
                raise Exception("Not allowed data key: %s" % _key)

        # Store the exit info into a .opt file
        _opt = os.path.join(conf.gate_path_opts, jid)
        with open(_opt, 'w') as _f:
            json.dump(data, _f)

    def get_flag(self, jid, flag):
        """
        Get job boolean flag. Can be used to mark job for future actions.

        :param jid: Job ID,
        :param flag: Name of the flag. Allowed values: stop, delete
        :return: Value of the flag (True or False)
        """
        _flags = ("stop", "delete")
        if flag not in _flags:
            raise Exception("Unknown flag: %s" % flag)

        return os.path.exists(os.path.join(conf.gate_path[flag], jid))

    def set_flag(self, jid, flag, value=True):
        """
        Set job boolean flag. Can be used to mark job for future actions.

        :param jid: Job ID,
        :param flag: Name of the flag. Allowed values: stop, delete
        :param value: of the flag (True or False)
        """
        _flags = ("stop", "delete")
        if flag not in _flags:
            raise Exception("Unknown flag: %s" % flag)

        _path = os.path.join(conf.gate_path[flag], jid)
        if value:
            # Mark new state in the shared file system
            os.symlink(
                os.path.join(conf.gate_path_jobs, jid),
                _path
            )
        else:
            os.unlink(_path)

    def delete_job(self, jid):
        # Remove job symlinks
        for _state, _path in conf.gate_path.items():
            _name = os.path.join(_path, jid)
            if os.path.exists(_name):
                os.unlink(_name)
        # @TODO Remove time stamps
        # Remove job file after symlinks (otherwise os.path.exists
        # fails on symlinks)
        os.unlink(os.path.join(conf.gate_path_jobs, jid))
        # Remove output status file
        _name = os.path.join(conf.gate_path_exit, jid)
        if os.path.exists(_name):
            os.unlink(_name)

        # Remove job from the list
        del JobStore[jid]

StateManager = StateManager()
