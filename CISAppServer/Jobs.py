#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""

"""

import os
import json
import logging
from subprocess import Popen, PIPE, STDOUT

from Config import conf, verbose, ExitCodes


logger = logging.getLogger(__name__)


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
            self.__check_state()
        except:
            self.die(u"@Job - Unable to check state (%s)." % self.id,
                     exc_info=True)
            return None

        # Load job internal data
        _name = os.path.join(conf.gate_path_opts, self.id)
        if os.path.isfile(_name):
            try:
                with open(_name) as _f:
                    _data = json.load(_f)
                    if "exit_state" in _data.keys():
                        self.__exit_state = _data["exit_state"]
                    if "exit_code" in _data.keys():
                        self.__exit_code = _data["exit_code"]
                    if "exit_message" in _data.keys():
                        self.__exit_message = _data["exit_message"]
            except:
                logger.error("@Job - Unable to load job info from: %s." %
                             _name)

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

    def cleanup(self):
        """ Mark job as in cleanup state. """
        self.__set_state('cleanup')

    def delete(self):
        """ Mark job for removal. """
        # Mark new state in the shared file system
        os.symlink(
            os.path.join(conf.gate_path_jobs, self.id),
            os.path.join(conf.gate_path_delete, self.id)
        )

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
        self.__set_state("closing")

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

    def __check_state(self):
        """
        Query the shared storage to identify current job state.

        In normal situation job remembers its state internally. However when
        restarting AppServer after shutdown with already running jobs this will
        allow to reload them.
        """

        if os.path.exists(os.path.join(conf.gate_path_aborted, self.id)):
            self.__state = 'aborted'
        elif os.path.exists(os.path.join(conf.gate_path_killed, self.id)):
            self.__state = 'killed'
        elif os.path.exists(os.path.join(conf.gate_path_failed, self.id)):
            self.__state = 'failed'
        elif os.path.exists(os.path.join(conf.gate_path_done, self.id)):
            self.__state = 'done'
        elif os.path.exists(os.path.join(conf.gate_path_cleanup, self.id)):
            self.__state = 'cleanup'
        elif os.path.exists(os.path.join(conf.gate_path_closing, self.id)):
            self.__state = 'closing'
        elif os.path.exists(os.path.join(conf.gate_path_running, self.id)):
            self.__state = 'running'
        elif os.path.exists(os.path.join(conf.gate_path_queued, self.id)):
            self.__state = 'queued'
        elif os.path.exists(os.path.join(conf.gate_path_waiting, self.id)):
            self.__state = 'waiting'

    def __set_state(self, new_state):
        """
        Change job state.

        :param new_state: new sate for the job. For valid states see
            :py:meth:`get_state`.
        """
        logger.debug("@Job - Set new state: %s (%s)" % (new_state, self.id))

        if new_state not in conf.service_states:
            raise Exception("Unknown job state %s (%s)." %
                            (new_state, self.id))

        self.get_state()  # TODO - check_state ???

        if self.__state == new_state:
            return

        # Mark new state in the shared file system
        os.symlink(
            os.path.join(conf.gate_path_jobs, self.id),
            os.path.join(conf.gate_path[new_state], self.id)
        )

        # Remove all other possible states just in case we previously failed
        for _state in conf.service_states:
            if _state != new_state:
                _name = os.path.join(conf.gate_path[_state], self.id)
                if os.path.exists(_name):
                    os.unlink(_name)

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

                # Store the exit info into a .opt file
                _opt = os.path.join(conf.gate_path_opts, self.id)
                try:
                    with open(_opt, 'w') as _f:
                        _data = {
                            "exit_state": self.__exit_state,
                            "exit_code": self.__exit_code,
                            "exit_message": self.__exit_message
                        }
                        json.dump(_data, _f)
                except:
                    if self.__exit_state != 'aborted':
                        raise
                    else:
                        logger.error('@Job - Unable to store job internal'
                                     'state', exc_info=True)
