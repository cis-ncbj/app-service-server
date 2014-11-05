#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""

"""

import os
import logging
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy import event, create_engine, func
from sqlalchemy.orm import relationship, backref, sessionmaker, deferred, \
        joinedload, Session
from sqlalchemy import Column, Integer, String, DateTime, PickleType, ForeignKey

from Config import conf, VERBOSE, ExitCodes
from DataStore import SchedulerStore, ServiceStore


logger = logging.getLogger(__name__)

class UnicodeBase(object):
    """
    This class can be used by ``declarative_base``, to add an automatic
    ``__unicode__`` method to *all* subclasses of ``Base``. This ``__unicode__`` will
    represent values as::

        ClassName(col_1=value_1, col_2=value_2, ..., col_n=value_n)

    where ``col_1..col_n`` are the columns of the mapped table with the
    corresponding values.
    """
    def __unicode__(self):
        cols = self.__table__.columns
        items = [(_.name, getattr(self, _.name))
                 for _ in cols]
        return u"{0}({1})".format(
            self.__class__.__name__,
            u', '.join([u'{0}={1!r}'.format(*_) for _ in items]))

# Base class for SQLAlchemy ORM objects
Base = declarative_base(cls=UnicodeBase)


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
    id = Column(String, unique=True)
    #: Current job state
    state = Column(String)
    #: Jobs' Service
    service = Column(String)
    #: Jobs' Scheduler
    scheduler = Column(String)
    #: Job exit message
    exit_message = deferred(Column(String))
    #: Job exit state
    exit_state = Column(Integer)
    #: Job exit code
    exit_code = deferred(Column(Integer))
    #: Time when job was submitted
    submit_time = Column(DateTime)
    #: Time when job execution started
    start_time = deferred(Column(DateTime))
    #: Time when job execution ended
    stop_time = Column(DateTime)
    #: Time when wait flag was set
    wait_time = deferred(Column(DateTime))

    # Flag values
    FLAG_DELETE, FLAG_STOP, FLAG_WAIT_QUOTA, FLAG_WAIT_INPUT, FLAG_OLD_API, \
        FLAG_ALL = \
        (1<<x for x in range(0,6))
    FLAG_ALL -= 1
    #: Job flags: delete, stop, wait_quita, wait_input, old_api
    flags = Column(Integer)
    #: Dirty status of job flags
    flags_dirty = Column(Integer)

    # Values of dirty flags
    D_ID, D_SERVICE, D_SCHEDULER, D_STATE, D_EXIT_MESSAGE, D_EXIT_STATE, D_EXIT_CODE, \
        D_SUBMIT_TIME, D_START_TIME, D_STOP_TIME, D_WAIT_TIME, D_FLAGS, \
        D_ALL = \
        (1<<x for x in range(0,13))
    D_ALL -= 1
    attr_dirty = Column(Integer)

    def __init__(self, id, service=None, scheduler=None, state=None, exit_message=None,
                 exit_state=None, exit_code=None, submit_time=None, start_time=None,
                 stop_time=None, wait_time=None, flags=0):
        #: Dirty flags
        self.attr_dirty = 0
        self.flags_dirty = 0
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
        # Reset dirty flags
        self.attr_dirty = 0
        self.flags_dirty = 0

# Listeners to "set" events for JobState attributes. They set dirty flags that
# are used to sync the changes with AppGw.
@event.listens_for(JobState.id, 'set')
def set_job_state_id(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_ID


@event.listens_for(JobState.service, 'set')
def set_job_state_service(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_SERVICE


@event.listens_for(JobState.scheduler, 'set')
def set_job_state_scheduler(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_SCHEDULER


@event.listens_for(JobState.state, 'set')
def set_job_state_state(target, value, oldvalue, initiator):
    logger.log(VERBOSE, "@JobState - Dirty: state changed (%s => %s)",
            oldvalue, value)
    target.attr_dirty |= target.D_STATE


@event.listens_for(JobState.exit_message, 'set')
def set_job_state_exit_message(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_EXIT_MESSAGE


@event.listens_for(JobState.exit_state, 'set')
def set_job_state_exit_state(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_EXIT_STATE


@event.listens_for(JobState.exit_code, 'set')
def set_job_state_exit_code(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_EXIT_CODE


@event.listens_for(JobState.submit_time, 'set')
def set_job_state_submit_time(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_SUBMIT_TIME


@event.listens_for(JobState.start_time, 'set')
def set_job_state_start_time(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_START_TIME


@event.listens_for(JobState.stop_time, 'set')
def set_job_state_stop_time(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_STOP_TIME


@event.listens_for(JobState.wait_time, 'set')
def set_job_state_wait_time(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_WAIT_TIME


@event.listens_for(JobState.flags, 'set')
def set_job_state_flags(target, value, oldvalue, initiator):
    target.attr_dirty |= target.D_FLAGS
    # For new objects oldvalue is not set
    if isinstance(oldvalue, int):
        _diff = value ^ int(oldvalue)
    else:
        _diff = value
    target.flags_dirty |= _diff

# Listeners to "delete" event for JobState instances. Call the cleanup.
@event.listens_for(JobState, 'after_delete')
def delete_job_state(mapper, connection, target):
    StateManager.cleanup(target)

@event.listens_for(Session, 'before_flush')
def flush_session(thissession, flush_context, instances):
    logger.verbose("Flush of the ORM state to DB")


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
    id = Column(String)


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
    size = deferred(Column(Integer))
    # Data stored in related tables. We use the relationship to make sure
    # related rows will be removed when job is removed
    # Specify explicitly foreign key for JobState. We do not want to use the
    # foreign key constraint so that JobState instances can be inserted into
    # seperate table.
    status_key = Column(Integer, ForeignKey('job_states.key'))
    #: Job state synchronized with AppGateway
    status = relationship("JobState", backref=backref('jobs', uselist=False),
            cascade="all, delete-orphan", single_parent=True, lazy="joined")
    #: Job parameters after validation
    data = relationship("JobData", uselist=False, backref='jobs',
            cascade="all, delete-orphan", single_parent=True)
    #: Stores scheduler related info for jobs that were passed to the scheduler
    scheduler = relationship("SchedulerQueue", uselist=False, backref='jobs',
            cascade="all, delete-orphan", single_parent=True)
    #: List of job IDs whose output we would like to consume (job chaining)
    chain = relationship("JobChain", backref='jobs',
            cascade="all, delete-orphan", single_parent=True)

    def __init__(self, job_id, job_state=None):
        """
        Works with existing job requests that are identified by their unique ID
        string.

        :param job_id: The unique job ID.
        :param job_state: Existing JobState instance to associate with the new
            Job.
        """
        logger.log(VERBOSE, "@Job - Job initialization")
        if job_state is None:
            job_state = JobState(job_id)
        elif not isinstance(job_state, JobState):
            raise Exception("Unknown JobState type: %s." % type(job_state))
        elif job_state.id != job_id:
            raise Exception("Inconsistent job IDs: %s != %s" % (job_state.id, job_id))

        _service = None
        for _name in ServiceStore.keys():
            if job_id.startswith(_name):
                _service = _name
        if _service is None:
            raise Exception("Unknon Job service for job: %s." % job_id)
        else:
            job_state.service = _service

        self.status = job_state
        self.size = 0
        self.status.exit_code = ExitCodes.Undefined
        self.status.state = 'waiting'
        self.status.submit_time = datetime.utcnow()

    def id(self):
        """ Get the job unique identifier. """
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
        * processing:
            job request was submitted and JobManager is processing it,
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

    def wait(self):
        """ Mark job as in waiting state. """
        self.__set_state('waiting')

    def processing(self):
        """ Mark job as in processing state. """
        self.__set_state('processing')

    def queue(self):
        """ Mark job as queued. """
        self.__set_state('queued')

    def run(self):
        """ Mark job as running. """
        self.__set_state('running')
        self.status.start_time = datetime.utcnow()

    def cleanup(self):
        """ Mark job as in cleanup state. """
        self.__set_state('cleanup')

    def delete(self):
        """ Mark job for removal. """
        self.set_flag(JobState.FLAG_DELETE)

    def mark(self, message, exit_code=ExitCodes.UserKill):
        """
        Mark job as killed by user.

        :param message: that will be passed to user,
        :param exit_code: one of :py:class:`ExitCodes`.
        """
        # Mark as killed makes sense only for unfinished jobs
        if self.get_state() not in ['waiting', 'queued', 'running']:
            logger.warning("@Job - Job %s already finished, cannot mark as "
                           "killed", self.id())
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
            self.status.state = 'aborted'

    def exit(self):
        """
        Finalise job cleanup by setting the state and passing the exit message
        to the user. Should be called only after Job.finish() method was
        called.
        """

        logger.log(VERBOSE, "@Job - Finish job %s.", self.id())
        if self.status.exit_state is None:
            self.die("@Job - Exit status is not defined for job %s.",
                     self.id())
            return

        # Generate the output status file
        try:
            self.__set_state(self.status.exit_state)
        except:
            logger.error("@Job - Cannot set job state %s.", self.id(),
                         exc_info=True)

        # Store stop time
        try:
            self.status.stop_time = datetime.utcnow()
        except:
            logger.error("@Job - Cannot store job stop time.", exc_info=True)

        logger.info("Job %s finished: %s", self.id(), self.status.exit_state)

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
                logger.log(VERBOSE, "@Job - Job output size calculated: 0")
                return

            # Use /usr/bin/du as os.walk is very slow
            _opts = [u'/usr/bin/du', u'-sb', _name]
            logger.log(VERBOSE, "@Job - Running command: %s", _opts)
            _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
            _output = _proc.communicate()
            logger.log(VERBOSE, _output)
            # Hopefully du returned meaningful result
            _size = _output[0].split()[0]
            # Check return code. If du was not killed by signal Popen will
            # not rise an exception
            if _proc.returncode != 0:
                raise OSError((
                    _proc.returncode,
                    "/usr/bin/du returned non zero exit code.\n%s",
                    str(_output)
                ))
        except:
            logger.error(
                "@Job - Unable to calculate job output size %s.", self.id(),
                exc_info=True
            )
            return

        self.size = int(_size)
        logger.log(VERBOSE, "@Job - Job output size calculated: %s", self.size)

    def compact(self):
        """
        Release resources allocated for the job
        """
        self.data = None

    def set_flag(self, flag, remove=False):
        """
        Set a job flag.

        Valid flags are defined in :py:class:`JobState`:
            FLAG_DELETE, FLAG_STOP, FLAG_WAIT_QUOTA, FLAG_WAIT_INPUT,
            FLAG_OLD_API, FLAG_ALL

        :param flag: a flag or set of flags to set. e.g.
            job.set_flag(JobState.FLAG_WAIT_QUOTA | JobState.FLAG_WAIT_INPUT)
        :param remove: if False the flag bit is set to ON,
            it is set to OFF otherwise
        """
        if flag <=0 or flag > JobState.FLAG_ALL:
            raise Exception("Unknown job flags %s (%s)." %
                            (flag, self.id()))
        if remove:
            self.status.flags &= ~flag
        else:
            self.status.flags |= flag

    def get_flag(self, flag):
        """
        Get a value of job flag.

        Valid flags are defined in :py:class:`JobState`:
            FLAG_DELETE, FLAG_STOP, FLAG_WAIT_QUOTA, FLAG_WAIT_INPUT,
            FLAG_OLD_API, FLAG_ALL

        :param flag: a flag or set of flags to check. e.g.
            job.get_flag(JobState.FLAG_WAIT_QUOTA)
        :return: the value of a flag.  In case of multiple flags like
            (JobState.FLAG_WAIT_QUOTA | JobState.FLAG_WAIT_INPUT)
            the return value is 1 when all flags are ON and 0 otherwise.
        """
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
        logger.log(VERBOSE, "@Job - State changed to %s(%s).", new_state, self.id())

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
                if self.status.exit_message is not None:
                    self.status.exit_message += _message
                else:
                    self.status.exit_message = _message


class StateManager(object):
    """
    Interface for persistent storage of job states and management of the job
    queue.

    Based on SQLite database allows for creation, menagement and querying of
    jobs.
    """

    def __init__(self):
        self.session_factory = None
        self.session_factory_noflush = None
        self.session = None

    def init(self):
        '''Initialize StateManager. Should be done explicitely after
        configuration is loaded.'''
        # Performing this at __init__ with the module singleton pattern will
        # run the initialization before configuration and logging is setup
        _log_levels = {
                'ERROR': logging.ERROR,
                'WARN': logging.WARN,
                'INFO': logging.INFO,
                'DEBUG': logging.DEBUG,
                'VERBOSE': logging.DEBUG
                }

        #: Turn on DB logging for verbose mode
        logging.getLogger('sqlalchemy.engine').setLevel(
                _log_levels[conf.log_level_db])
        logging.getLogger('sqlalchemy.orm').setLevel(
                _log_levels[conf.log_level_db])
        #: DB engine
        self.engine = create_engine(conf.config_db)
        # Enforce foreign keys in SQLite
        self.engine.execute('pragma foreign_keys=on')
        self.engine.execute('pragma journal_mode=WAL')
        #: Session factory
        self.session_factory = sessionmaker()
        self.session_factory.configure(bind=self.engine)
        self.session_factory_noflush = sessionmaker(autoflush=False)
        self.session_factory_noflush.configure(bind=self.engine)
        #: DB session handle
        self.session = self.session_factory()
        # Create the tables in the DB
        #TODO what if the DB exists?
        Base.metadata.create_all(self.engine)

    def new_session(self, autoflush=True):
        """
        Create new DB session.

        :return: session instance.
        """
        if not autoflush:
            return self.session_factory_noflush()
        return self.session_factory()

    def expire_session(self, session=None):
        """
        Expire session cache. Will reload all objects from the DB on next
        access.

        :param session: if specified use this session instance instead of the
            default.
        """
        if session is None:
            session = self.session
        session.expire_all()

    def commit(self, session=None):
        """
        Commit changes to the DB. Invalidates SQLAlchemy ORM instances.

        :param session: if specified use this session instance instead of the
            default.
        """
        logger.log(VERBOSE, "@StateManager: Commit session to DB")
        if session is None:
            session = self.session

        # Issue rollback in case of problems.
        # Otherwise SQLAlchemy is in inconsistent state and raises exceptions.
        try:
            session.commit()
        except:
            session.rollback()
            raise

    def flush(self, session=None):
        """
        Flush changes to the DB transaction. The data are not persisted (or
        available for other transactions) until a call to commit.

        :param session: if specified use this session instance instead of the
            default.
        """
        logger.log(VERBOSE, "@StateManager: Flush session to DB")
        if session is None:
            session = self.session

        # Issue rollback in case of problems.
        # Otherwise SQLAlchemy is in inconsistent state and raises exceptions.
        try:
            session.flush()
        except:
            logger.error("@StateManager - Flush failed.", exc_info=True)
            session.rollback()

    def poll_gw(self, session=None):
        """
        Check for changes in job states issued by the AppGW.

        If found merge them into the DB.

        :param session: if specified use this session instance instead of the
            default.
        """
        raise NotImplementedError

    def cleanup(self, status):
        """
        Cleanup AppGW state after Job removal.

        :param status: JobState instance.
        """
        raise NotImplementedError

    def new_job(self, job_id):
        """
        Create new Job instance with job_id identifier and attach it to DB
        session.

        Depending on the implementation can query the AppGW and set the proper
        job state.

        :param job_id: the unique job identifier.
        :return: the new Job instance.
        """
        raise NotImplementedError

    def attach_job(self, job, session=None):
        """
        Attach a Job instance to the DB session.

        :param job: the Job instance.
        :param session: if specified use this session instance instead of the
            default.
        """
        if session is None:
            session = self.session
        logger.log(VERBOSE, "@StateManager: Will attach job %s to the session.", job.id())
        session.add(job)

    def detach_job(self, job, session=None):
        """
        Attach a Job instance to the DB session.

        :param job: the Job instance.
        :param session: if specified use this session instance instead of the
            default.
        """
        if session is None:
            session = self.session
        logger.log(VERBOSE, "@StateManager: Will detach job %s from session.", job.id())
        # Make sure no changes are pending
        self.commit(session)
        session.expunge(job)

    def merge_job(self, job, session=None):
        """
        Merge state of a Job instance into the DB session.

        :param job: the Job instance.
        :param session: if specified use this session instance instead of the
            default.
        """
        if session is None:
            session = self.session
        # Make sure no changes are pending
        self.commit(session)
        session.merge(job)

    def get_job(self, job_id, session=None):
        """
        Get Job object from the job queue.

        :param job_id: Job unique ID,
        :param session: if specified use this session instance instead of the
            default.
        :return: Job object for *job_id*, None if job_id was not found in the
            queue.
        """
        if session is None:
            session = self.session

        return session.query(Job).join(JobState).filter(JobState.id == job_id).one()

    def get_new_job_list(self):
        """
        Query AppGW for a list of jobs that are in a *new* state.

        For each job in *new* state a Job instance will be created and attached
        to the default DB session. The jobs are set into waiting state.

        :return: a list of Job instances that were in the *new* state.
        """
        raise NotImplementedError

    def get_job_list(self, state="all", service=None, scheduler=None,
            flag=None, session=None):
        """
        Get a list of jobs that are in a selected state.

        :param state: Specifies state for which Jobs will be selected. To
            select all jobs specify 'all' as the state. Valid states consist of
            job states and flags: waiting, processing, queued, running,
            closing, cleanup, done, failed, aborted, killed
        :param service: if specified select only jobs that belong to selected
            service.
        :param flag: if specified select only jobs with the flag (or set of
            flags) set to ON. Valid flags are defined in :py:class:`JobSTate`.
        :param session: if specified use this session instance instead of the
            default.

        :return: List of Job instances sorted by submit time.
        :raises:
        """
        if session is None:
            session = self.session

        # Validate input
        if scheduler is not None and scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s.", scheduler
            )
            return
        if state != 'all' and state not in conf.service_states:
            logger.error("@StateManager - Unknown state: %s", state)
            return
        if service is not None and service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s", service)
            return
        if flag is not None and flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s", flag)
            return

        # Build query
        _q = session.query(Job).join(JobState) 
        if scheduler is not None:
            _q = _q.join(SchedulerQueue).\
                 filter(SchedulerQueue.scheduler == scheduler)
        if state != 'all':
            _q = _q.filter(JobState.state == state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag is not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        # Order results by submit time
        _q = _q.order_by(JobState.submit_time)
        # Execute query
        return _q.all()

    def get_job_list_byid(self, ids, session=None, full=False):
        """
        Get a list of jobs that are in a selected state.

        :param ids:

        :return: List of Job instances sorted by submit time.
        :raises:
        """
        if session is None:
            session = self.session

        # Validate input
        if not len(ids):
            logger.error("Empty list of job IDS.")
            return

        # Build query
        logger.debug("Build job query for ids: %s", ids)
        _q = session.query(Job).join(JobState)
        if full:
            _q = _q.options(
                    joinedload(Job.data),
                    joinedload(Job.chain),
                    joinedload(Job.scheduler)
                    )
        _q = _q.filter(JobState.id.in_(ids))
        # Order results by submit time
        _q = _q.order_by(JobState.submit_time)
        logger.debug("Complete query: %s", str(_q))
        # Execute query
        return _q.all()

    def get_job_count(self, state="all", service=None, scheduler=None,
                      flag=None, session=None):
        """
        Get a count of jobs that are in a selected state.

        :param str state: Specifies state for which Jobs will be selected. To
            select all jobs specify 'all' as the state. Valid states consist of
            job states and flags: waiting, processing, queued, running,
            closing, cleanup, done, failed, aborted, killed
        :param str service: if specified select only jobs that belong to
            selected service.
        :param int flag: if specified select only jobs with the flag (or set of
            flags) set to ON. Valid flags are defined in :py:class:`JobSTate`.
        :param Session session: if specified use this session instance instead
            of the default.

        :return: Number of jobs in the selected state, or -1 in case of error.
        """
        if session is None:
            session = self.session

        _job_count = -1

        # Validate input
        if scheduler is not None and scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s.", scheduler
            )
            return _job_count
        if state != 'all' and state not in conf.service_states:
            logger.error("@StateManager - Unknown state: %s", state)
            return _job_count
        if service is not None and service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s", service)
            return _job_count
        if flag is not None and flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s", flag)
            return _job_count

        # Build query
        _q = session.query(Job).join(JobState) 
        if scheduler is not None:
            _q = _q.join(SchedulerQueue).\
                 filter(SchedulerQueue.scheduler == scheduler)
        if state != 'all':
            _q = _q.filter(JobState.state == state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag is not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        # Execute query
        try:
            return _q.count()
        except:
            logger.error(u"@StateManager - Unable count jobs.", exc_info=True)
            return _job_count

    def get_active_job_count(self, service=None, scheduler=None, flag=None,
                      session=None):
        """
        Get a count of jobs that are active.

        :param str service: if specified select only jobs that belong to
            selected service.
        :param int flag: if specified select only jobs with the flag (or set of
            flags) set to ON. Valid flags are defined in :py:class:`JobSTate`.
        :param Session session: if specified use this session instance instead
            of the default.

        :return: Number of jobs in the selected state, or -1 in case of error.
        """
        if session is None:
            session = self.session

        _job_count = -1

        # Validate input
        if scheduler is not None and scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s.", scheduler
            )
            return _job_count
        if service is not None and service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s", service)
            return _job_count
        if flag is not None and flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s", flag)
            return _job_count

        _counters = self.get_job_state_counters(service=service,
                scheduler=scheduler, flag=flag, session=session)
        _states = { _key : 0 for _key in conf.service_states }
        for (_count, _key) in _counters:
            _states[_key] = _count

        _job_count = 0
        for _key in ("queued", "processing", "running", "closing", "cleanup"):
            _job_count += _states[_key]

        return _job_count

    def get_job_state_counters(self, service=None, scheduler=None, flag=None, session=None):
        """
        Get a count of jobs that are in a selected state.

        :param str service: if specified select only jobs that belong to
            selected service.
        :param int flag: if specified select only jobs with the flag (or set of
            flags) set to ON. Valid flags are defined in :py:class:`JobSTate`.
        :param Session session: if specified use this session instance instead
            of the default.

        :return: Number of jobs in the selected state, or -1 in case of error.
        """
        if session is None:
            session = self.session

        _job_count = []

        # Validate input
        if scheduler is not None and scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s.", scheduler
            )
            return _job_count
        if service is not None and service not in ServiceStore.keys():
            logger.error("@StateManager - Unknown service: %s", service)
            return _job_count
        if flag is not None and flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s", flag)
            return _job_count

        # Build query
        if scheduler is not None:
            _q = session.query(func.count(Job), JobState.state).join(JobState)
            _q = _q.join(SchedulerQueue).\
                 filter(SchedulerQueue.scheduler == scheduler)
        else:
            _q = session.query(func.count(JobState.state), JobState.state)
        if service is not None:
            _q = _q.filter(JobState.service == service)
        if flag is not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        _q = _q.group_by(JobState.state)
        # Execute query
        try:
            return _q.all()
        except:
            logger.error(u"@StateManager - Unable count jobs.", exc_info=True)
            return _job_count

    def get_active_service_counters(self, scheduler=None, flag=None, session=None):
        """
        Get a count of jobs that are in a selected state.

        :param str service: if specified select only jobs that belong to
            selected service.
        :param int flag: if specified select only jobs with the flag (or set of
            flags) set to ON. Valid flags are defined in :py:class:`JobSTate`.
        :param Session session: if specified use this session instance instead
            of the default.

        :return: Number of jobs in the selected state, or -1 in case of error.
        """
        if session is None:
            session = self.session

        _job_count = []

        # Validate input
        if scheduler is not None and scheduler not in SchedulerStore.keys():
            logger.error(
                "@StateManager - unknown scheduler %s.", scheduler
            )
            return _job_count
        if flag is not None and flag <= 0 or flag > JobState.FLAG_ALL:
            logger.error("@StateManager - Unknown flag: %s", flag)
            return _job_count

        # Build query
        if scheduler is not None:
            _q = session.query(func.count(JobState.service), JobState.service, Job).join(Job)
            _q = _q.join(SchedulerQueue).\
                 filter(SchedulerQueue.scheduler == scheduler)
        else:
            _q = session.query(func.count(JobState.service), JobState.service)
        if flag is not None:
            _q = _q.filter(JobState.flags.op('&')(flag) > 0)
        _q = _q.filter(
                (JobState.state == 'queued') |
                (JobState.state == 'processing') |
                (JobState.state == 'running') |
                (JobState.state == 'closing') |
                (JobState.state == 'cleanup'))
        _q = _q.group_by(JobState.service)
        # Execute query
        try:
            return _q.all()
        except:
            logger.error(u"@StateManager - Unable count jobs.", exc_info=True)
            return _job_count

    def remove_flags(self, flag, service='all', session=None):
        """
        Clear flags for jobs that belong to selected service.

        :param flag: The flag to clear.
        :param service: Name of the affected service. If equals to "all" flag
          is cleared for every job.
        :throws:
        """
        if session is None:
            session = self.session

        # Input validation
        if service != 'all' and service not in ServiceStore.keys():
            raise Exception("@StateManager - Unknown service %s." % service)
        if flag <= 0 or flag > JobState.FLAG_ALL:
            raise Exception("Unknown flag: %s" % flag)

        _q = session.query(Job).join(JobState)
        if service != 'all':
            _q = _q.filter(JobState.service == service)
        _list = _q.all()

        for _job in _list:
            _job.set_flag(flag, remove=True)

        # @TODO Test the update code instead of select+modify approach.
        #       Requires session.commit to be in consisten state?? synchronize_session takes care of this??
        #_q = session.query(JobState)
        #if service != 'all':
        #    _q = _q.filter(JobState.service == service)
        #_q.update(values={
        #    JobState.flags: JobState.flags &= ~flag,
        #    JobState.flags_dirty: JobState.flags_dirty &= ~flag
        #    }, sychronize_session='fetch')

    def delete_job(self, job, session=None):
        '''
        Remove all job persistant data. Except for the output directory.

        :param jid: Job ID
        :throws:
        '''
        if session is None:
            session = self.session

        session.delete(job)

class FileStateManager(StateManager):
    def new_job(self, jid, session=None):
        _job = Job(jid)
        self.attach_job(_job, session)
        return _job

    def get_new_job_list(self):
        logger.log(VERBOSE, u"@FileStateManager - Query job requests")

        _path = conf.gate_path_new
        try:
            _list = os.listdir(_path)
        except:
            logger.error(u"@FileStateManager - Unable to read directory: %s." %
                         _path, exc_info=True)
            return

        logger.log(VERBOSE, u"@FileStateManager - New job requests: %s", len(_list))

        # Limit number of jobs to process in one go
        if len(_list) > (conf.config_batch_jobs * conf.config_max_threads):
            _list = _list[0:(conf.config_batch_jobs * conf.config_max_threads)]
        for _jid in _list:
            # Create new Job instance. It will be created in 'waiting' state
            _job = self.new_job(_jid)
            if _job is None:
                continue

        # Get list of waiting jobs (includes new requests and request not processed yet)
        _jobs = self.get_job_list("waiting")

        logger.log(VERBOSE, u"@FileStateManager - Waiting job requests: %s", len(_jobs))

        return _jobs

    def commit(self, session=None):
        logger.log(VERBOSE, "@FileStateManager: Push status changes to AppGW.")
        if session is None:
            session = self.session

        try:
            session.flush()
        except:
            session.rollback()
            raise

        for _entry in session.query(JobState).filter(JobState.attr_dirty > 0):
            logger.log(VERBOSE, "@FileStateManager: Found dirty (%s) JobState (%s).",
                    _entry.attr_dirty, _entry.id)
            if _entry.attr_dirty & JobState.D_SERVICE:
                self.__service_change(_entry)
            if _entry.attr_dirty & JobState.D_STATE:
                self.__state_change(_entry)
            if _entry.attr_dirty & JobState.D_EXIT_MESSAGE:
                self.__exit_message_change(_entry)
            if _entry.attr_dirty & JobState.D_EXIT_STATE:
                self.__exit_state_change(_entry)
            if _entry.attr_dirty & JobState.D_EXIT_CODE:
                self.__exit_code_change(_entry)
            if _entry.attr_dirty & JobState.D_SUBMIT_TIME:
                self.__submit_time_change(_entry)
            if _entry.attr_dirty & JobState.D_START_TIME:
                self.__start_time_change(_entry)
            if _entry.attr_dirty & JobState.D_STOP_TIME:
                self.__stop_time_change(_entry)
            if _entry.attr_dirty & JobState.D_WAIT_TIME:
                self.__wait_time_change(_entry)
            if _entry.attr_dirty & JobState.D_FLAGS:
                self.__flags_change(_entry)
            _entry.attr_dirty = 0

        super(FileStateManager, self).commit(session)

    def poll_gw(self, session=None):
        logger.log(VERBOSE, u"@FileStateManager: Poll AppGW for status changes.")
        if session is None:
            session = self.session

        # Kill flags
        _path = conf.gate_path_flag_stop
        try:
            _list = os.listdir(_path)
        except:
            logger.error(u"@FileStateManager - Unable to read directory: %s." %
                         _path, exc_info=True)
            return
        logger.log(VERBOSE, u"@FileStateManager: Obtained job kill flags.")

        #@TODO should we use session??
        for _job in self.get_job_list(flag=JobState.FLAG_STOP):
            if _job.id() in _list:
                _list.remove(_job.id())

        for _id in _list:
            _job = self.get_job(_id)
            _job.set_flag(JobState.FLAG_STOP)
        logger.log(VERBOSE, u"@FileStateManager: Jobs flagged for kill.")

        # Delete flags
        _path = conf.gate_path_flag_delete
        try:
            _list = os.listdir(_path)
        except:
            logger.error(u"@FileStateManager - Unable to read directory: %s." %
                         _path, exc_info=True)
            return
        logger.log(VERBOSE, u"@FileStateManager: Obtained job delete flags.")

        for _job in self.get_job_list(flag=JobState.FLAG_DELETE):
            if _job.id() in _list:
                _list.remove(_job.id())

        for _id in _list:
            _job = self.get_job(_id)
            _job.set_flag(JobState.FLAG_DELETE)
        logger.log(VERBOSE, u"@FileStateManager: Jobs flagged for delete.")

    def cleanup(self, status):
        """
        Cleanup after job removal.

        Removes the flags and other files representing job state.

        :param status: JobState instance
        """
        _jid = status.id

        logger.log(VERBOSE, u"@FileStateManager: Job cleanup (%s)", _jid)
        # Remove job symlinks
        for _state, _path in conf.gate_path.items():
            _name = os.path.join(_path, _jid)
            if os.path.exists(_name):
                os.unlink(_name)
        # Remove job file after symlinks (otherwise os.path.exists
        # fails on symlinks)
        os.unlink(os.path.join(conf.gate_path_jobs, _jid))
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
        """
        Propagate the job state to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id
        _new_state = status.state

        logger.log(VERBOSE, "@FileStateManager: State changed to: %s (%s)",
                _new_state, _jid)
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
        """
        Propagate the exit message to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Store exit msg: %s (%s)",
                status.exit_message, _jid)
        # The auxiliary info is stored in the opts directory as files with
        # names concatanated from data type name and job ID.
        _opt = os.path.join(conf.gate_path_opts, 'message_' + _jid)
        with open(_opt, 'w') as _f:
            _f.write(status.exit_message)

    def __exit_state_change(self, status):
        """
        Propagate the exit state to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Store exit state: %s (%s)",
                status.exit_state, _jid)
        # The auxiliary info is stored in the opts directory as files with
        # names concatanated from data type name and job ID.
        _opt = os.path.join(conf.gate_path_opts, 'state_' + _jid)
        with open(_opt, 'w') as _f:
            _f.write(status.exit_state)

    def __exit_code_change(self, status):
        """
        Propagate the exit code to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Store exit code: %s (%s)",
                status.exit_code, _jid)
        # The auxiliary info is stored in the opts directory as files with
        # names concatanated from data type name and job ID.
        _opt = os.path.join(conf.gate_path_opts, 'code_' + _jid)
        with open(_opt, 'w') as _f:
            _f.write("%s" % status.exit_code)

    def __submit_time_change(self, status):
        """
        Propagate the submit timestamp to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Submit time change: %s (%s)",
                status.submit_time, _jid)
        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "submit_" + _jid)
        # Calculate UNIX time stamp as number of seconds since epoch
        _tstamp = (status.submit_time - datetime(1970,1,1)).total_seconds()
        with file(_fname, 'a'):
            os.utime(_fname, (_tstamp, _tstamp))

    def __start_time_change(self, status):
        """
        Propagate the start timestamp to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Start time change: %s (%s)",
                status.start_time, _jid)
        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "start_" + _jid)
        # Calculate UNIX time stamp as number of seconds since epoch
        _tstamp = (status.start_time - datetime(1970,1,1)).total_seconds()
        with file(_fname, 'a'):
            os.utime(_fname, (_tstamp, _tstamp))

    def __stop_time_change(self, status):
        """
        Propagate the stop timestamp to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Stop time change: %s (%s)",
                status.stop_time, _jid)
        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "stop_" + _jid)
        # Calculate UNIX time stamp as number of seconds since epoch
        _tstamp = (status.stop_time - datetime(1970,1,1)).total_seconds()
        with file(_fname, 'a'):
            os.utime(_fname, (_tstamp, _tstamp))

    def __wait_time_change(self, status):
        """
        Propagate the wait timestamp to the GW.

        :param ststus: JobState instace
        """
        _jid = status.id

        logger.log(VERBOSE, "@FileStateManager: Wait time change: %s (%s)",
                status.wait_time, _jid)
        # Timestamps are stored in the time directory as files with names
        # concatanated from event name and job ID.
        _fname = os.path.join(conf.gate_path_time, "wait_" + _jid)
        # Calculate UNIX time stamp as number of seconds since epoch
        _tstamp = (status.wait_time - datetime(1970,1,1)).total_seconds()
        with file(_fname, 'a'):
            os.utime(_fname, (_tstamp, _tstamp))

    def __flags_change(self, status):
        """
        Propagate the changes in flags to the GW.

        In case a flag was modified by both AppServer and the GW the value set
        by Server is used.

        :param status: - JobState instance
        """
        _jid = status.id
        _flags_add = []    # Flags to set
        _flags_remove = [] # Flags to remove

        logger.log(VERBOSE, "@FileStateManager: Flags change: %s (%s)",
                status.flags, _jid)
        # Consider only flags that were modified - have flags_dirty flag set
        # Flags that are currently set are added to _flags_add
        # Flags that are not set are added to _flags_remove
        if status.flags_dirty & JobState.FLAG_DELETE:
            _flag = 'flag_delete'
            if status.flags & JobState.FLAG_DELETE:
                _flags_add.append(_flag)
            else:
                _flags_remove.append(_flag)
        if status.flags_dirty & JobState.FLAG_STOP:
            _flag = 'flag_stop'
            if status.flags & JobState.FLAG_STOP:
                _flags_add.append(_flag)
            else:
                _flags_remove.append(_flag)
        if status.flags_dirty & JobState.FLAG_WAIT_QUOTA:
            _flag = 'flag_wait_quota'
            if status.flags & JobState.FLAG_WAIT_QUOTA:
                _flags_add.append(_flag)
            else:
                _flags_remove.append(_flag)
        if status.flags_dirty & JobState.FLAG_WAIT_INPUT:
            _flag = 'flag_wait_input'
            if status.flags & JobState.FLAG_WAIT_INPUT:
                _flags_add.append(_flag)
            else:
                _flags_remove.append(_flag)
        if status.flags_dirty & JobState.FLAG_OLD_API:
            _flag = 'flag_old_api'
            if status.flags & JobState.FLAG_OLD_API:
                _flags_add.append(_flag)
            else:
                _flags_remove.append(_flag)

        # Set new flags
        for _flag in _flags_add:
            _path = os.path.join(conf.gate_path[_flag], _jid)
            # Mark new state in the shared file system
            if not os.path.exists(_path):
                os.symlink(
                    os.path.join(conf.gate_path_jobs, _jid),
                    _path
                )

        # Remove old flags
        for _flag in _flags_remove:
            _path = os.path.join(conf.gate_path[_flag], _jid)
            if os.path.exists(_path):
                os.unlink(_path)

        # Remove the dirty flag
        status.flags_dirty = 0

# Singleton object
StateManager = FileStateManager()
