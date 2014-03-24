#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""

"""

import os
import json
import logging
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, PickleType

from Config import conf, verbose, ExitCodes
from DataStore import JobStore, SchedulerStore, ServiceStore


logger = logging.getLogger(__name__)

Base = declarative_base()


class JobBadStateError(Exception):
    """ Wrong job state exception. """


class MutableDict(Mutable, dict):
    """
    This class allows to store dict (which is mutable) as immutable column
    in DB (e.g. pickled string) using SQLAlchemy. The dict will be mutable in
    ORM and SQLAlchemy will correctly detect changes.
    """

    @classmethod
    def coerce(cls, key, value):
        if not isinstance(value, MutableDict):
            if isinstance(value, dict):
                return MutableDict(value)
            return Mutable.coerce(key, value)
        else:
            return value

    def __delitem(self, key):
        dict.__delitem__(self, key)
        self.changed()

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self.changed()

    def __getstate__(self):
        return dict(self)

    def __setstate__(self, state):
        self.update(self)


class JobState(Base):
    """
    Class that stores information about Job state that will be synchronized
    with AppGateway.
    """
    # Name of the table used by JobState instances 
    __tablename__ = 'job_states'

    # JobState uses declarative_base to define DB columns
    #: Primary key - autoincrement
    key = Column(Integer, primary_key=True)
    #: Foreign key (links to Job)
    #job_key = Column(Integer)
    #: Job unique identifier
    id = Column(String)
    #: Jobs' Service
    service = Column(String)
    #: Jobs' Scheduler
    scheduler = Column(String)
    #: Current job state
    state = Column(String)
    #: Job exit message
    exit_message = Column(String)
    #: Job exit state
    exit_state = Column(Integer)
    #: Job exit code
    exit_code = Column(Integer)
    #: Time when job was submitted
    submit_time = Column(DateTime)
    #: Time when job execution started
    start_time = Column(DateTime)
    #: Time when job execution ended
    stop_time = Column(DateTime)
    #: Time when wait flag was set
    wait_time = Column(DateTime)

    # Flag values
    FLAG_DELETE, FLAG_STOP, FLAG_WAIT_QUOTA, FLAG_WAIT_INPUT, FLAG_OLD_API, \
        FLAG_ALL = \
        (1<<x for x in range(0,5))
    FLAG_ALL -= 1
    #: Job flags: delete, stop, wait_quita, wait_input, old_api
    flags = Column(Integer)

    # Values of dirty flags
    D_ID, D_SERVICE, D_SCHEDULER, D_STATE, D_EXIT_MESSAGE, D_EXIT_STATE, D_EXIT_CODE, \
        D_SUBMIT_TIME, D_START_TIME, D_STOP_TIME, D_WAIT_TIME, D_FLAGS, \
        D_ALL = \
        (1<<x for x in range(0,12))
    D_ALL -= 1

    def __init__(self, id, service=None, scheduler=None, state=None, exit_message=None,
                 exit_state=None, exit_code=None, submit_time=None, start_time=None,
                 stop_time=None, wait_time=None, flags=0):
        self.id = id
        self.service = service
        self.scheduler = scheduler
        self.state = state
        self.exit_message = exit_message
        self.exit_state = exit_state
        self.exit_code = exit_code
        self.submit_time = submit_time
        self.start_time = start_time
        self.stop_time = stop_time
        self.wait_time = wait_time
        self.flags = flags
        #: Dirty flags
        self.dirty = 0

    # Initialize of load from DB
    @orm.reconstructor
    def init_on_load(self):
        self.dirty = 0

# Listeners to "set" events for JobState attributes. They set dirty flags that
# are used to sync the changes with AppGw.
@event.listens_for(JobState.id, 'set')
def set_job_state_id(target, value, oldvalue, initiator):
    target.dirty |= target.D_ID


@event.listens_for(JobState.service, 'set')
def set_job_state_service(target, value, oldvalue, initiator):
    target.dirty |= target.D_SERVICE


@event.listens_for(JobState.scheduler, 'set')
def set_job_state_scheduler(target, value, oldvalue, initiator):
    target.dirty |= target.D_SCHEDULER


@event.listens_for(JobState.state, 'set')
def set_job_state_state(target, value, oldvalue, initiator):
    target.dirty |= target.D_STATE


@event.listens_for(JobState.exit_message, 'set')
def set_job_state_exit_message(target, value, oldvalue, initiator):
    target.dirty |= target.D_EXIT_MESSAGE


@event.listens_for(JobState.exit_state, 'set')
def set_job_state_exit_state(target, value, oldvalue, initiator):
    target.dirty |= target.D_EXIT_STATE


@event.listens_for(JobState.exit_code, 'set')
def set_job_state_exit_code(target, value, oldvalue, initiator):
    target.dirty |= target.D_EXIT_CODE


@event.listens_for(JobState.submit_time, 'set')
def set_job_state_submit_time(target, value, oldvalue, initiator):
    target.dirty |= target.D_SUBMIT_TIME


@event.listens_for(JobState.start_time, 'set')
def set_job_state_start_time(target, value, oldvalue, initiator):
    target.dirty |= target.D_START_TIME


@event.listens_for(JobState.stop_time, 'set')
def set_job_state_stop_time(target, value, oldvalue, initiator):
    target.dirty |= target.D_STOP_TIME


@event.listens_for(JobState.wait_time, 'set')
def set_job_state_wait_time(target, value, oldvalue, initiator):
    target.dirty |= target.D_WAIT_TIME


@event.listens_for(JobState.flags, 'set')
def set_job_state_flags(target, value, oldvalue, initiator):
    target.dirty |= target.D_FLAGS


class JobData(Base):
    """
    Class that stores Job validated input data.
    """
    # Name of the table used by JobData instances 
    __tablename__ = 'job_data'

    # JobData uses declarative_base to define DB columns
    #: Primary key - autoincrement
    key = Column(Integer, primary_key=True)
    #: Foreign key (links to Job)
    job_key = Column(Integer, ForeignKey('jobs.key'))
    #: Pickled dictionary with Job validated settings
    data = Column(MutableDict.as_mutable(PickleType))


class JobChain(Base):
    """
    Class that stores chained Job IDs.
    """
    # Name of the table used by JobChain instances 
    __tablename__ = 'job_chain'

    # JobChain uses declarative_base to define DB columns
    #: Primary key - autoincrement
    key = Column(Integer, primary_key=True)
    #: Foreign key (links to Job)
    job_key = Column(Integer, ForeignKey('jobs.key'))
    #: Id of Job chained as input
    id = Column(Strig)


class SchedulerQueue(Base):
    """
    Class that stores scheduler related Job data.
    """
    # Name of the table used by SchedulerQueue instances 
    __tablename__ = 'scheduler_queue'

    # SchedulerQueue uses declarative_base to define DB columns
    #: Primary key - autoincrement
    key = Column(Integer, primary_key=True)
    #: Foreign key (links to Job)
    job_key = Column(Integer, ForeignKey('jobs.key'))
    #: Scheduler job ID
    id = Column(Integer)
    #: Scheduler
    scheduler = Column(String)
    #: Queue
    queue = Column(String)


class Job(Base):
    """
    Class that implements a job instance.

    Allows to:

    * query/set job state,
    * generate the output status file,
    * query job parameters defined in job request.

    Class is based of SQLAlchemy declarative.Base and allows for easy
    persistance and searching of the Jobs instances.
    """
    # Name of the table used by Job instances 
    __tablename__ = 'jobs'

    # Job uses declarative_base to define DB columns
    #: Primary key - autoincrement
    key = Column(Integer, primary_key=True)
    #: Job output size in bytes
    size = Column(Integer)
    # Data stored in related tables. We use the relationship to make sure
    # related rows will be removed when job is removed
    #: Job state synchronized with AppGateway
    # Specify explicitly foreign key for JobState. We do not want to use the
    # foreign key constraint so that JobState instances can be inserted into
    # seperate table.
    #status = relationship("JobState", uselist=False, backref='jobs', cascade="all, delete, delete-orphan",
    #                      primaryjoin='Job.key==JobState.job_key', foreign_keys='JobState.job_key')
    status_key = Column(Integer, ForeignKey('job_states.key'))
    status = relationship("JobState", backref=backref('jobs', uselist=False), cascade="all, delete, delete-orphan", single_parent=True)
    #: Job parameters after validation
    valid_data = relationship("JobData", uselist=False, backref='jobs', cascade="all, delete, delete-orphan", single_parent=True)
    #: Stores scheduler related info for jobs that were passed to the scheduler 
    scheduler = relationship("SchedulerQueue", uselist=False, backref='jobs', cascade="all, delete, delete-orphan", single_parent=True)
    #: List of job IDs whose output we would like to consume (job chaining)
    chain = relationship("JobChain", backref='jobs', cascade="all, delete, delete-orphan", single_parent=True)

    def __init__(self, job_id, job_state=None):
        """
        Works with existing job requests that are identified by their unique ID
        string. Upon initialization loads job parameters from job request JSON
        file.

        :param job_id: The unique job ID.
        """
        if job_state is None:
            job_state = JobState(job_id)
        elif job_state.id != job_id:
            raise Exception("Inconsistent job IDs: %s != %s" % (job_state.id, job_id)
        if not isinstance(job_state, JobState):
            raise Exception("Unknown JobState type: %s." % type(job_state))
        self.status = job_state
        self.size = 0  # Job output size in bytes
        self.status.exit_code = ExitCodes.Undefined  # Job exit code
        self.status.state = 'waiting'
        self.status.submit_time = datetime.now()

    def id(self):
        return self.status.id

    def get_size(self):
        """
        Get the size of job output directory.
        Requires that calculate_size was called first.

        :return: Size of output directory in bytes.
        """
        return self.size

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

        return self.status.state

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

        return self.status.exit_state

    def queue(self):
        """ Mark job as queued. """
        self.__set_state('queued')

    def run(self):
        """ Mark job as running. """
        self.__set_state('running')
        self.status.start_time = datetime.now()

    def cleanup(self):
        """ Mark job as in cleanup state. """
        self.__set_state('cleanup')

    def delete(self):
        """ Mark job for removal. """
        self.set_flag(self, self.FLAG_DELETE)

    def mark(self, message, exit_code=ExitCodes.UserKill):
        """
        Mark job as killed by user.

        :param message: that will be passed to user,
        :param exit_code: one of :py:class:`ExitCodes`.
        """
        # Mark as killed makes sense only for unfinished jobs
        if self.get_state() not in ['waiting', 'queued', 'running']:
            logger.warning("@Job - Job %s already finished, cannot mark as "
                           "killed" % self.id())
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
                         self.id(), exc_info=True)
            self.status.state = 'aborted' #TODO sync with appGW??

    def exit(self):
        """
        Finalise job cleanup by setting the state and passing the exit message
        to the user. Should be called only after Job.finish() method was
        called.
        """

        if self.status.exit_state is None:
            self.die("@Job - Exit status is not defined for job %s." %
                     self.id())
            return

        #TODO write exit message in set_exit_message
        # Generate the output status file
        try:
            self.__set_state(self.status.exit_state)
        except:
            logger.error("@Job - Cannot set job state %s." % self.id(),
                         exc_info=True)

        # Store stop time
        try:
            self.status.stop_time = datetime.now()
        except:
            logger.error("@Job - Cannot store job stop time.", exc_info=True)

        logger.info("Job %s finished: %s" % (self.id(), self.status.exit_state))

    def calculate_size(self):
        """
        Calculate size of output directory of the job if it exists.
        """
        self.size = 0

        # Only consider job states that could have an output dir
        if self.status.state not in \
           ('cleanup', 'done', 'failed', 'killed', 'abort'):
            return

        try:
            # Check that output dir exists
            _name = os.path.join(conf.gate_path_output, self.id())
            if not os.path.exists(_name):
                self.size = 0
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
                "@Job - Unable to calculate job output size %s." % self.id(),
                exc_info=True
            )
            return

        self.size = int(_size)
        verbose("@Job - Job output size calculated: %s" % self.size)

    def compact(self):
        """
        Release resources allocated for the job (data, valid_data).
        """
        self.data = None

    def set_flag(self, flag, remove=False):
        if flag <=0 or flag > self.FLAG_ALL:
            raise exception("unknown job flags %s (%s)." %
                            (flag, self.id()))
        if remove:
            self.status.flags |= ~flag
        else:
            self.status.flag |= flag

    def get_flag(self, flag):
        return (self.status.flags & flag) > 0

    def __set_state(self, new_state):
        """
        Change job state.

        :param new_state: new sate for the job. For valid states see
            :py:meth:`get_state`.
        """
        if new_state not in conf.service_states:
            raise Exception("Unknown job state %s (%s)." %
                            (new_state, self.id()))

        self.status.state = new_state

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
        if self.status.exit_state != 'aborted':
            if self.status.exit_state != 'killed' or state == 'aborted':
                self.status.exit_state = state
                self.status.exit_code = exit_code
                # Concatanate all status messages
                self.status.exit_message += _message


class StateManager(object):
    """
    Interface for persistent storage of job states.

    Default implementation uses files on shared file system.
    """

    def __init__(self):        
        engine = create_engine('sqlite:///:memory:', echo=True)
        engine.execute('pragma foreign_keys=on')
        Session = sessionmaker()
        Session.configure(bind=engine)
        #: DB session handle
        self.session = Session()

    def commit(self):
        self.session.commit()
    
    def new_job(self, job_id):
	raise NotImplementedError

    def add_job(self, job):
        self.session.add(job)

    def get_job(self, job_id):
        """
        Get Job object from JobStore.

        :param job_id: Job unique ID,
        :return: Job object for *job_id*.
        :throws: NoResultFound exception if job_id is not recognized. 
        """

        return self.session.query(Job).join(JobState).filter(JobState.id == job_id).one()

    def get_new_job_list(self):
	raise NotImplementedError

    def get_job_list(self, state="all", service=None, flag=None):
        """
        Get a list of jobs that are in a selected state.

	:param state: Specifies state for which Jobs will be selected. To
	    select all jobs specify 'all' as the state. Valid states consist of
	    job states and flags: waiting, queued, running, closing, cleanup,
            done, failed, aborted, killed

        :return: List of Job instances sorted by submit time.
        """
        if state != 'all' and state not in conf.service_states:
            logger.error("@StateManager - Unknown state: %s" % state)
            return
        if service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s" % service)
            return
        if flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s" % flag)
            return
       
        _q = self.session.query(Job).join(JobState) 
        if state != 'all':
            _q = _q.filter(JobState.state == state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag in not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        _q = _q.order_by(JobState.submit_time)
        return _q.all()

    def get_job_count(self, state="all", service=None, flag=None):
        """
        Get a count of jobs that are in a selected state.

	:param state: Specifies state for which Jobs will be selected. To
	    select all jobs specify 'all' as the state. Valid states consist of
	    job states and flags: waiting, queued, running, closing, cleanup,
            done, failed, aborted, killed

        :return: Number of jobs in the selected state, or -1 in case of error.
        """
        _job_count = -1

        if state != 'all' and state not in conf.service_states:
            logger.error("@StateManager - Unknown state: %s" % state)
            return _job_count
        if service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s" % service)
            return _job_count
        if flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s" % flag)
            return _job_count
       
        _q = self.session.query(Job).join(JobState) 
        if state != 'all':
            _q = _q.filter(JobState.state == state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag in not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        try:
	    return _q.count()
        except:
            logger.error(u"@StateManager - Unable count jobs.", exc_info=True)
            return _job_count

    def get_scheduler_list(self, scheduler, state="all", service=None, flag=None):
        """
        Get a list of jobs that are in a scheduler queue.

        :param scheduler: Specifies scheduler name for which Jobs will be
            selected.

        :return: List of Job instances sorted by submit time.
        """

	if scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s." % scheduler
            )
            return
        if state != 'all' and state not in conf.service_states:
            logger.error("@StateManager - Unknown state: %s" % state)
            return
        if service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s" % service)
            return
        if flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s" % flag)
            return
       
        _q = self.session.query(Job).join(JobState).join(SchedulerQueue).\
                 filter(SchedulerQueue.scheduler == scheduler)
        if state != 'all':
            _q = _q.filter(JobState.state == state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag in not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        _q = _q.order_by(JobState.submit_time)
	return _q.all()

    def get_scheduler_count(self, scheduler, state="all", service=None, flag=None):
        """
        Get a count jobs that are in a scheduler queue.

        :param scheduler: Specifies scheduler name for which Jobs will be
            selected.

        :return: Number of jobs in the scheduler queue, or -1 in case of error.
        """
        _job_count = -1

	if scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s." % scheduler
            )
            return _job_count
        if state != 'all' and state not in conf.service_states:
            logger.error("@StateManager - Unknown state: %s" % state)
            return _job_count
        if service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s" % service)
            return _job_count
        if flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s" % flag)
            return _job_count
       
        _q = self.session.query(Job).join(JobState).join(SchedulerQueue).\
                 filter(SchedulerQueue.scheduler == scheduler)
        if state != 'all':
            _q = _q.filter(JobState.state == state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag in not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        try:
	    return _q.count()
        except:
            logger.error(u"@StateManager - Unable count jobs.", exc_info=True)
            return _job_count

    def remove_flags(self, flag, service='all'):
        """
        Clear flags for jobs that belong to selected service.

        :param flag: The flag to clear.
        :param service: Name of the affected service. If equals to "all" flag
          is cleared for every job.
        :throws:
        """
        # Input validation
        if service != 'all' or service not in ServiceStore.keys():
            raise Exception("@StateManager - Unknown service %s." % service)
        if flag <= 0 or flag > JobState.FLAG_ALL:
            raise Exception("Unknown flag: %s" % flag)

        _q = self.session.query(Job).join(JobState)
        if service != 'all':
            _q = _q.filter(JobState.service == service)
        _list = _q.all()
        # TODO run update instead of select? What about triggers?

        for _job in _list:
            _job.set_flag(flag, remove=True)

    def delete_job(self, job):
        '''
        Remove all job persistant data. Except for the output directory.

        :param jid: Job ID
        :throws:
        '''
        self.session.delete(job)

class FileStateManager(StateManager):
    def new_job(self, jid):
        _job = Job(jid)
        self.add_job(_job)
        return _job

    def get_new_job_list(self):
        _jobs = []
        try:
            _list = os.listdir(_path)
        except:
            logger.error(u"@FileStateManager - Unable to read directory: %s." %
                         _path, exc_info=True)
            return

        for _jid in _list:
            _job = self.new_job(_jid)
            if _job is None:
                continue
            _jobs.append(_job)

        _jobs.extend(self.get_job_list("waiting"))

        return _jobs

    def commit(self):
        for _entry in self.session.dirty:
            if isinstance(_entry, JobState):
                if _entry.dirty & JobState.D_SERVICE:
                    self.__service_change(_entry)
                if _entry.dirty & JobState.D_STATE:
                    self.__state_change(_entry)
                if _entry.dirty & JobState.D_EXIT_MESSAGE:
                    self.__exit_message_change(_entry)
                if _entry.dirty & JobState.D_EXIT_STATE:
                    self.__exit_state_change(_entry)
                if _entry.dirty & JobState.D_EXIT_CODE:
                    self.__exit_code_change(_entry)
                if _entry.dirty & JobState.D_SUBMIT_TIME:
                    self.__submit_time_change(_entry)
                if _entry.dirty & JobState.D_START_TIME:
                    self.__start_time_change(_entry)
                if _entry.dirty & JobState.D_STOP_TIME:
                    self.__stop_time_change(_entry)
                if _entry.dirty & JobState.D_WAIT_TIME:
                    self.__wait_time_change(_entry)
                if _entry.dirty & JobState.D_FLAGS:
                    self.__flags_change(_entry)
                _entry.dirty = 0

        for _entry in self.session.deleted:
            if isinstance(_entry, JobState):
                __cleanup(_entry)

        super(FileStateManager, self).commit()

    def __cleanup(self, status):
        _jib = status.id

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
        # Remove time stamps
        _list = os.listdir(conf.gate_path_time)
        for _item in _list:
            if _item.endswith(_jid):
                _name = os.path.join(conf.gate_path_time, _item)
                os.unlink(_name)
        # Remove persistent data
        _list = os.listdir(conf.gate_path_opts)
        for _item in _list:
            if _item.endswith(_jid):
                _name = os.path.join(conf.gate_path_opts, _item)
                os.unlink(_name)

    def __service_change(self, status):
        pass

    def __state_change(self, status):
        _jid = status.id
        _new_state = status.state

        # Mark new state in the shared file system
        os.symlink(
            os.path.join(conf.gate_path_jobs, _jid),
            os.path.join(conf.gate_path[_new_state], _jid)
        )

        # Remove all other possible states just in case we previously failed
        for _state in conf.service_states:
            if _state != _new_state:
                _name = os.path.join(conf.gate_path[_state], _jid)
                if os.path.exists(_name):
                    os.unlink(_name)

    def __exit_message_change(self, status):
        _jid = status.id

        # Store the auxiliary info into an .opt file
        _opt = os.path.join(conf.gate_path_opts, 'message_' + _jid)
        with open(_opt, 'w') as _f:
            _f.write(status.exit_message)

    def __exit_state_change(self, status):
        _jid = status.id

        # Store the auxiliary info into an .opt file
        _opt = os.path.join(conf.gate_path_opts, 'state_' + _jid)
        with open(_opt, 'w') as _f:
            _f.write(status.exit_state)

    def __exit_code_change(self, status):
        _jid = status.id

        # Store the auxiliary info into an .opt file
        _opt = os.path.join(conf.gate_path_opts, 'code_' + _jid)
        with open(_opt, 'w') as _f:
            _f.write(status.exit_code)

    def __submit_time_change(self, status):
        _jid = status.id

        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "submit_" + _jid)
        with file(_fname, 'a'):
            os.utime(_fname, time_stamp)

    def __start_time_change(self, status):
        _jid = status.id

        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "start_" + _jid)
        with file(_fname, 'a'):
            os.utime(_fname, time_stamp)

    def __stop_time_change(self, status):
        _jid = status.id

        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "stop_" + _jid)
        with file(_fname, 'a'):
            os.utime(_fname, time_stamp)

    def __wait_time_change(self, status):
        _jid = status.id

        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "wait_" + _jid)
        with file(_fname, 'a'):
            os.utime(_fname, time_stamp)

    def __flags_change(self, status):
        _jid = status.id
        _flags_add = []
        _flags_remove = []

        _flag = 'flag_delete'
        if status.flags & JobState.FLAG_DELETE:
            _flags_add.append(_flag)
        else:
            _flags_remove.append(_flag)
        _flag = 'flag_stop'
        if status.flags & JobState.FLAG_STOP:
            _flags_add.append(_flag)
        else:
            _flags_remove.append(_flag)
        _flag = 'flag_wait_quota'
        if status.flags & JobState.FLAG_WAIT_QUOTA:
            _flags_add.append(_flag)
        else:
            _flags_remove.append(_flag)
        _flag = 'flag_wait_input'
        if status.flags & JobState.FLAG_WAIT_INPUT:
            _flags_add.append(_flag)
        else:
            _flags_remove.append(_flag)
        _flag = 'flag_old_api'
        if status.flags & JobState.FLAG_OLD_API:
            _flags_add.append(_flag)
        else:
            _flags_remove.append(_flag)

        for _flag in _flags_add:
	    _path = os.path.join(conf.gate_path[_flag], jid)
            # Mark new state in the shared file system
            if not os.path.exists(_path):
                os.symlink(
                    os.path.join(conf.gate_path_jobs, jid),
                    _path
                )

        for _flag in _flags_remove:
	    _path = os.path.join(conf.gate_path[_flag], jid)
            if os.path.exists(_path):
                os.unlink(_path)

StateManager = FileStateManager()
