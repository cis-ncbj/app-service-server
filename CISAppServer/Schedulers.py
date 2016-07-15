# -*- coding: UTF-8 -*-
"""
Module with implementations of the Scheduler interface.
"""

import os
import xml.etree.cElementTree as ET
import stat
import re
import shutil
import logging
import spur
import threading
import random

# Import subprocess32 module from pip
from subprocess32 import Popen, PIPE, STDOUT, TimeoutExpired

from jinja2 import Environment, FileSystemLoader, Template

from sqlalchemy.exc import SQLAlchemyError

import Jobs  # Import full module - resolves circular dependencies
import Globals as G
from Config import conf, VERBOSE, ExitCodes
from Tools import rollback

logger = logging.getLogger(__name__)


class CisError(Exception):
    """ Wrong job input. """


def rmtree_error(function, path, exc_info):
    """Exception handler for shutil.rmtree()."""

    logger.error("@rmtree - Cannot remove job output: %s %s" %
                 (function, path), exc_info=exc_info)


class Scheduler(object):
    """
    Virtual Class implementing simple interface for execution backend. Actual
    execution backends should derive from this class and implement defined
    interface.

    Allows for job submission, deletion and extraction of job status.
    """

    # We do not want to output progress with every status check. To lighten the
    # burden lets do it every n-th step
    __progress_step = 0

    def __init__(self):
        #: Working directory path
        self.work_path = None
        #: Path where submitted job IDs are stored
        self.queue_path = None
        #: Default queue
        self.default_queue = None
        #: Maximum number of concurrent jobs
        self.max_jobs = None
        #: Jinja2 environment configuration
        self.template_env = Environment(
            loader=FileSystemLoader(conf.service_path_data),
            variable_start_string=r'@@{',
            variable_end_string=r'}',
            trim_blocks=True,
            lstrip_blocks=True
        )
        #: StateManager instance
        self.state_manager = Jobs.state_manager_factory("FileStateManager")
        self.state_manager.init()

    def submit(self, job):
        """
        Submit a job for execution. The "pbs.sh" script should be already
        present in the work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """
        raise NotImplementedError

    def progress(self, job):
        """
        Extract the job progress log and expose it to the user.

        :param job: :py:class:`Job` instance
        """
        # Output job progress log if it exists
        # The progres log is extraced every n-th status check
        if Scheduler.__progress_step >= conf.config_progress_step:
            logger.log(VERBOSE, "@Scheduler - Extracting progress log (%s)",
                       job.id())
            _work_dir = os.path.join(self.work_path, job.id())
            _output_dir = os.path.join(conf.gate_path_output, job.id())
            _progress_file = os.path.join(_work_dir, 'progress.log')
            if os.path.exists(_progress_file):
                try:
                    if not os.path.isdir(_output_dir):
                        os.mkdir(_output_dir)
                    shutil.copy(_progress_file, _output_dir)
                    logger.log(VERBOSE, '@Scheduler - Progress log extracted')
                except:
                    logger.error(
                        '@Scheduler - Cannot copy progress.log',
                        exc_info=True
                    )
            Scheduler.__progress_step = 0
        else:
            Scheduler.__progress_step += 1

    def stop(self, job, msg, exit_code):
        """Stop running job and remove it from execution queue."""
        raise NotImplementedError

    def finalise(self, job):
        """
        Prepare output of finished job.

        Job working directory is moved to the external_data_path directory.

        :param job: :py:class:`Job` instance
        """

        _jid = job.id()
        _clean = True

        logger.debug("@Scheduler - Retrive job output: %s", _jid)
        _work_dir = os.path.join(self.work_path, _jid)

        # Cleanup of the output
        try:
            for _chain in job.chain:
                shutil.rmtree(os.path.join(_work_dir, _chain.id),
                              ignore_errors=True)
        except:
            logger.error("@Scheduler - Unable to clean up job output "
                         "directory %s", _work_dir, exc_info=True)

        if os.path.isdir(_work_dir):
            try:
                # Remove output dir if it exists.
                _out_dir = os.path.join(conf.gate_path_output, _jid)
                _dump_dir = os.path.join(conf.gate_path_dump, _jid)
                if os.path.isdir(_out_dir):
                    logger.debug('@Scheduler - Remove existing output directory')
                    # out and dump should be on the same partition so that rename
                    # is used. This will make sure that processes reading from out
                    # will not cause rmtree to throw exceptions
                    shutil.move(_out_dir, _dump_dir)
                    shutil.rmtree(_dump_dir, ignore_errors=True)
                shutil.move(_work_dir, conf.gate_path_output)

                # Make sure all files in the output directory are world readable -
                # so that apache can actually serve them
                logger.debug('@Scheduler - Make output directory world readable')
                os.chmod(_out_dir, os.stat(_out_dir).st_mode |
                         stat.S_IRUSR | stat.S_IXUSR |
                         stat.S_IRGRP | stat.S_IXGRP |
                         stat.S_IROTH | stat.S_IXOTH)
                for _root, _dirs, _files in os.walk(_out_dir):
                    for _dir in _dirs:
                        _name = os.path.join(_root, _dir)
                        os.chmod(_name, os.stat(_name).st_mode |
                                 stat.S_IRUSR | stat.S_IXUSR |
                                 stat.S_IRGRP | stat.S_IXGRP |
                                 stat.S_IROTH | stat.S_IXOTH)
                    for _file in _files:
                        _name = os.path.join(_root, _file)
                        try:
                            os.chmod(_name, os.stat(_name).st_mode |
                                 stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                        except Exception as e:
                            print e
            except:
                job.die("@Scheduler - Unable to retrive job output directory %s" %
                        _work_dir, exc_info=True)
                _clean = False

        if not _clean:
            return

        # Remove job from scheduler queue if it was queued
        try:
            job.scheduler = None
        except:
            logger.error("@Scheduler - Unable to remove job SchedulerQueue.",
                         exc_info=True)

        # Set job exit state
        job.exit()

        logger.debug("@Scheduler - job finalised: %s", _jid)

    def abort(self, job):
        """
        Cleanup aborted job.

        Removes working and output directories.

        :param job: :py:class:`Job` instance
        """

        _jid = job.id()

        logger.debug("Cleanup job: %s", _jid)

        _work_dir = os.path.join(self.work_path, _jid)
        _out_dir = os.path.join(conf.gate_path_output, _jid)
        _clean = True

        # Remove output dir if it exists.
        if os.path.isdir(_out_dir):
            logger.debug('Remove existing output directory')
            shutil.rmtree(_out_dir, ignore_errors=True)
        # Remove work dir if it exists.
        if os.path.isdir(_work_dir):
            logger.debug('Remove working directory')
            shutil.rmtree(_work_dir, ignore_errors=True)

        if os.path.isdir(_out_dir):
            _clean = False
            logger.error("Unable to remove job output directory: "
                         "%s", _jid, exc_info=True)
        if os.path.isdir(_work_dir):
            _clean = False
            logger.error("Unable to remove job working "
                         "directory: %s", _jid, exc_info=True)
        if _clean:
            logger.info("Job %s cleaned directories.", _jid)

        # Remove job from scheduler queue if it was queued
        try:
            job.scheduler = None
        except:
            logger.error("Unable to remove job SchedulerQueue.",
                         exc_info=True)
            raise

        logger.debug("Finalize cleanup: %s", _jid)
        try:
            # Set job exit state
            job.exit()
        except:
            logger.error("Unable to finalize cleanup.",
                         exc_info=True)
            raise
        logger.debug("Job abort finished: %s", _jid)

    def generate_scripts(self, job):
        """
        Generate scripts and job input files from templates.

        Will walk through service_data_path/service and copy all files
        including recursion through subdirectories to PBS work directory for
        specified job (the directory structure is retained). For all files
        substitute all occurences of @@{atribute_name} with specified values.

        :param job: :py:class:`Job` instance after validation
        :return: True on success and False otherwise.
        """

        # Input directory
        _script_dir = os.path.join(conf.service_path_data, job.status.service)
        # Output directory
        _work_dir = os.path.join(self.work_path, job.id())

        # Verify that input dir exists
        if not os.path.isdir(_script_dir):
            job.die("@Scheduler - Service data directory not found: %s." %
                    _script_dir)
            return False

        # Verify that input dir contains "pbs.sh" script
        if not os.path.isfile(os.path.join(_script_dir, 'pbs.sh')):
            job.die("@Scheduler - Missing \"pbs.sh\" script for service %s." %
                    job.status.service)
            return False

        # Create output dir
        if not os.path.isdir(_work_dir):
            try:
                os.mkdir(_work_dir)
            except IOError:
                job.die("@Scheduler - Unable to create WORKDIR %s." %
                        _work_dir, exc_info=True)
                return False

        # Recurse through input dir
        logger.debug("@Scheduler - generate scripts")
        for _path, _dirs, _files in os.walk(_script_dir):
            # Relative paths for subdirectories
            _sub_dir = os.path.relpath(_path, _script_dir)
            logger.debug("@Scheduler - Sub dir: %s", _sub_dir)
            if _sub_dir != '.':
                _out_dir = os.path.join(_work_dir, _sub_dir)
            else:
                _out_dir = _work_dir

            # Create subdirectories in output dir
            for _dir in _dirs:
                _name = os.path.join(_out_dir, _dir)
                try:
                    os.mkdir(_name)
                except:
                    job.die(
                        "@Scheduler - Cannot create job subdirectory %s." %
                        _name, exc_info=True
                    )
                    return False

            # Iterate through script files in current subdir
            for _file in _files:
                # Skip editor buffers and recovery files
                if _file.endswith('~'):
                    continue
                if _file.startswith('.') and _file.endswith('.swp'):
                    continue
                # Input and output file names
                _fin_name = os.path.join(_path, _file)
                _fou_name = os.path.join(_out_dir, _file)
                _template_name = os.path.join(job.status.service, _sub_dir, _file)
                try:
                    # Open input template script and output file
                    template = self.template_env.get_template(_template_name)
                    _fou = open(_fou_name, 'w')

                    #_fou.write(template.render(job.data.data))
                    _fou.writelines(template.generate(job.data.data))
                    _fou.close()
                    # Copy file permisions
                    _st = os.stat(_fin_name)
                    os.chmod(_fou_name, _st.st_mode)
                except TypeError:
                    job.die(
                        "@Scheduler - Scripts creation failed for job: %s." %
                        job.id(), exc_info=True
                    )
                    return False

        # Make sure that "pbs.sh" is executable
        try:
            _st = os.stat(os.path.join(_script_dir, 'pbs.sh'))
            os.chmod(os.path.join(_work_dir, 'pbs.sh'),
                     _st.st_mode | stat.S_IXUSR)
        except:
            job.die(
                "@Scheduler - Unable to change permissions for pbs.sh: %s." %
                job.id(), exc_info=True
            )
            return False

        # Make sure that "epilogue.sh" is executable
        # Make sure it is not writable by group and others - torque will
        # silently ignore it otherwise
        try:
            _st = os.stat(os.path.join(_script_dir, 'epilogue.sh'))
            os.chmod(os.path.join(_work_dir, 'epilogue.sh'),
                     (_st.st_mode | stat.S_IXUSR) &
                     (~stat.S_IWGRP & ~stat.S_IWOTH))
        except:
            job.die(
                "@Scheduler - Unable to change permissions for epilogue.sh: "
                "%s." % job.id(), exc_info=True
            )
            return False

        return True

    def chain_input_data(self, job):
        """
        Chain output data of finished jobs as our input.

        Copies output data of specified job IDs into the working directory of
        current job.

        :param job: :py:class:`Job` instance after validation
        :return: True on success and False otherwise.
        """
        logger.debug('@Scheduler - Chaining input data')
        _work_dir = os.path.join(self.work_path, job.id())
        for _cjob in job.chain:
            _id = _cjob.id
            _input_dir = os.path.join(conf.gate_path_output, _id)
            _output_dir = os.path.join(_work_dir, _id)
            if os.path.exists(_input_dir):
                try:
                    shutil.copytree(_input_dir, _output_dir)
                    logger.debug(
                        "@Scheduler - Job %s output chained as input for "
                        "Job %s", _id, job.id())
                except:
                    job.die(
                        '@Scheduler - Cannot chain job %s output.' % _id,
                        err=True, exc_info=True
                    )
                    return False
            else:
                job.die(
                    '@Scheduler - Job %s output directory does not exists.' %
                    _id
                )
                return False

        return True


class PbsScheduler(Scheduler):
    """
    Class implementing simple interface for PBS queue system.

    Allows for job submission, deletion and extraction of job status.
    """

    def __init__(self):
        super(PbsScheduler, self).__init__()
        #: PBS Working directory path
        self.work_path = conf.pbs_path_work
        #: Path where submitted PBS job IDs are stored
        self.queue_path = conf.pbs_path_queue
        #: Default PBS queue
        self.default_queue = conf.pbs_default_queue
        #: Maximum number of concurent jobs
        self.max_jobs = conf.pbs_max_jobs
        #: Scheduler name
        self.name = "pbs"

    def submit(self, job):
        """
        Submit a job to PBS queue. The "pbs.sh" script should be already
        present in the pbs_work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """

        # Check that maximum job limit is not exceeded
        # Use exclusive session to be thread safe, no need to pass the session
        # from JobManager
        _job_count = -1
        try:
            with Jobs.session_scope(self.state_manager) as _session:
                _job_count = self.state_manager.get_job_count(scheduler=self.name,
                                                             session=_session)
                if _job_count < 0:
                    return False
                if _job_count >= self.max_jobs:
                    return False

                # Path names
                _work_dir = os.path.join(self.work_path, job.id())
                _run_script = os.path.join(_work_dir, "pbs.sh")
                _output_log = os.path.join(_work_dir, "output.log")
                # Select queue
                _queue = G.SERVICE_STORE[job.status.service].config['queue']
                if 'CIS_QUEUE' in job.data.data:
                    _queue = job.data.data['CIS_QUEUE']

                try:
                    # Submit
                    logger.debug("@PBS - Submitting new job")
                    # Run qsub with proper user permissions
                    #
                    # @TODO
                    # Implementation of dedicated users for each service
                    # - The apprunner user currently does not allow to log in via ssh
                    # - either this limitation is lifted or the server will be run as
                    #    another user that is allowed to perform "sudo su apprunner"
                    #  - to execute sudo su a TTY is required
                    #  - this can be cirumvented using "ssh -t -t"
                    #   - A passwordless key is setup:
                    #    - .ssh/authorized_keys entry contains from="127.0.0.1" to
                    #      limit the scope of the key
                    #    - .ssh/config contains following entries to auto select the
                    #      correct key when connecting to the "local" host:
                    #     Host local
                    #         HostName 127.0.0.1
                    #         Port 22
                    #         IdentityFile ~/.ssh/id_dsa_local
                    #  - using this will generate TTY warning messages that are
                    #    suppressed only for OpenSSH 5.4 or newer. Hence the STDERR
                    #    should not be mixed with STDIN in this case
                    # - There still is a problem of file permisions:
                    #  - Data management is run with service users permisions
                    #  - Data is set to be group writable
                    #  - We use ACLs
                    # @TODO END
                    #
                    # _user = self.jm.services[job.service].config['username']
                    # _comm = "/usr/bin/qsub -q %s -d %s -j oe -o %s " \
                    # "-l epilogue=epilogue.sh %s" % \
                    # (_queue, _work_dir, _output_log, _run_script)
                    # _sudo = "/usr/bin/sudo /bin/su -c \"%s\" %s" % (_comm, _user)
                    # _opts = ['/usr/bin/ssh', '-t', '-t', 'local', _sudo]
                    _opts = ['/usr/bin/qsub', '-q', _queue, '-d', _work_dir, '-j',
                             'oe', '-o', _output_log, '-b', str(conf.pbs_timeout),
                             '-l', 'epilogue=epilogue.sh',
                             _run_script]
                    logger.log(VERBOSE, "@PBS - Running command: %s", _opts)
                    _proc = Popen(_opts, stdout=PIPE, stderr=PIPE)
                    _out = _proc.communicate()
                    _output = _out[0]
                    logger.log(VERBOSE, _out)
                    # Hopefully qsub returned meaningful job ID
                    _pbs_id = _output.strip()
                    # Check return code. If qsub was not killed by signal Popen will
                    # not rise an exception
                    if _proc.returncode != 0:
                        raise OSError((
                            _proc.returncode,
                            "/usr/bin/qsub returned non zero exit code.\n%s" %
                            str(_out)
                        ))
                except:
                    job.die("@PBS - Unable to submit job %s." % job.id(), exc_info=True)
                    return False

                # @TODO Do I need DB session here???
                # Store the PBS job ID
                job.scheduler = Jobs.SchedulerQueue(
                    scheduler=self.name, id=_pbs_id, queue=_queue)
                # Reduce memory footprint
                job.compact()

                logger.info("Job successfully submitted: %s", job.id())
        except:
            logger.error("@PBS - Unable to connect to DB.", exc_info=True)
        return True

    @rollback(SQLAlchemyError)
    def update(self, jobs):
        """
        Update job states to match their current state in PBS queue.

        :param jobs: A list of Job instances for jobs to be updated.
        """
        # Extract list of user names associated to the jobs
        _users = []
        for _service in G.SERVICE_STORE.values():
            if _service.config['username'] not in _users:
                _users.append(_service.config['username'])

        # We agregate the jobs by user. This way one qstat call per user is
        # required instead of on call per job
        _job_states = {}
        for _usr in _users:
            try:
                # Run qstat
                logger.log(VERBOSE, "@PBS - Check jobs state for user %s", _usr)
                _opts = ["/usr/bin/qstat", "-f", "-x", "-u", _usr]
                logger.log(VERBOSE, "@PBS - Running command: %s", _opts)
                _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
                # Requires subprocess32 module from pip
                _output = _proc.communicate(timeout=conf.pbs_timeout)[0]
                logger.log(VERBOSE, _output)
                # Check return code. If qstat was not killed by signal Popen
                # will not rise an exception
                if _proc.returncode != 0:
                    raise OSError((
                        _proc.returncode,
                        "/usr/bin/qstat returned non zero exit code.\n%s" %
                        str(_output)
                    ))
            except TimeoutExpired:
                _proc.kill()
                logger.error("@PBS - Unable to check jobs state.",
                             exc_info=True)
                return
            except:
                logger.error("@PBS - Unable to check jobs state.",
                             exc_info=True)
                return

            # Parse the XML output of qstat
            try:
                _xroot = ET.fromstring(_output)
                for _xjob in _xroot.iter('Job'):
                    _xjid = _xjob.find('Job_Id').text
                    _xstate = _xjob.find('job_state').text
                    _xexit = _xjob.find('exit_status')
                    if _xexit is not None:
                        _xexit = _xexit.text
                    _job_states[_xjid] = (_xstate, _xexit)
            except:
                logger.error("@PBS - Unable to parse qstat output.",
                             exc_info=True)
                return
            logger.log(VERBOSE, _job_states)

        # Iterate through jobs
        for _job in jobs:
            # TODO rewrite to get an array JID -> queue from SchedulerQueue table with single SELECT
            _pbs_id = str(_job.scheduler.id)
            # Check if the job exists in the PBS
            if _pbs_id not in _job_states:
                _job.die('@PBS - Job %s does not exist in the PBS', _job.id())
            else:
                # Update job progress output
                self.progress(_job)
                _new_state = 'queued'
                _exit_code = 0
                _state = _job_states[_pbs_id]
                logger.log(VERBOSE, "@PBS - Current job state: '%s' (%s)",
                           _state[0], _job.id())
                # Job has finished. Check the exit code.
                if _state[0] == 'C':
                    _new_state = 'done'
                    _msg = 'Job finished succesfully'

                    _exit_code = _state[1]
                    if _exit_code is None:
                        _new_state = 'killed'
                        _msg = 'Job was killed by the scheduler'
                        _exit_code = ExitCodes.SchedulerKill

                    _exit_code = int(_exit_code)
                    if _exit_code > 256:
                        _new_state = 'killed'
                        _msg = 'Job was killed by the scheduler'
                    elif _exit_code > 128:
                        _new_state = 'killed'
                        _msg = 'Job was killed'
                    elif _exit_code > 0:
                        _new_state = 'failed'
                        _msg = 'Job finished with error code'

                    try:
                        _job.finish(_msg, _new_state, _exit_code)
                    except:
                        _job.die('@PBS - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id()), exc_info=True)
                # Job is running
                elif _state[0] == 'R' or _state[0] == 'E':
                    if _job.get_state() != 'running':
                        try:
                            _job.run()
                        except:
                            _job.die("@PBS - Unable to set job state "
                                     "(running : %s)" % _job.id(), exc_info=True)
                # Treat all other states as queued
                else:
                    if _job.get_state() != 'queued':
                        try:
                            _job.queue()
                        except:
                            _job.die("@PBS - Unable to set job state "
                                     "(queued : %s)" % _job.id(), exc_info=True)

    def stop(self, job, msg, exit_code):
        """
        Stop running job and remove it from PBS queue.

        :param job: :py:class:`Job` instance
        :param msg: Message that will be passed to the user
        """
        _pbs_id = ''

        # Get Job PBS ID
        try:
            _pbs_id = str(job.scheduler.id)
        except:
            job.die('@PBS - Unable to read PBS job ID', exc_info=True)
            return

        # Run qdel
        logger.debug("@PBS - Killing job")
        # @TODO Seperate users for each sevice
        # Run qdel with proper user permissions
        # _user = self.jm.services[job.service].config['username']
        # _comm = "/usr/bin/qdel %s" % _pbs_id
        # _sudo = "/usr/bin/sudo /bin/su -c \"%s\" %s" % (_comm, _user)
        # _opts = ['/usr/bin/ssh', '-t', '-t', 'local', _sudo]
        _opts = ['/usr/bin/qdel', '-b', str(conf.pbs_timeout), _pbs_id]
        logger.log(VERBOSE, "@PBS - Running command: %s", _opts)
        _proc = Popen(_opts, stdout=PIPE, stderr=STDOUT)
        _output = _proc.communicate()[0]
        logger.log(VERBOSE, _output)
        # Check return code. If qdel was not killed by signal Popen will
        # not rise an exception
        # @TODO handle temporary communication timeouts with pbs server
        if _proc.returncode == 170:  # Job in wrong state (e.g. exiting)
            logger.debug("@PBS - Wait with job kill: /usr/bin/qdel "
                         "returned 170 exit code (%s)", _output)
            raise CisError("PBS qdel wrong job state")
        if _proc.returncode != 0:
            raise OSError((
                _proc.returncode,
                "/usr/bin/qdel returned non zero exit code.\n%s" %
                str(_output)
            ))

        # Mark as killed by user
        job.mark(msg, exit_code)


class SshScheduler(Scheduler):
    """
    Class implementing simple interface for job execution through SSH.

    Allows for job submission, deletion and extraction of job status.
    """

    def __init__(self):
        super(SshScheduler, self).__init__()
        #: SSH Working directory path
        self.work_path = conf.ssh_path_work
        #: Path where submitted SSH job IDs are stored
        self.queue_path = conf.ssh_path_queue
        #: Default SSH execute host
        self.default_queue = conf.ssh_default_queue
        #: Dict of maximum number of concurrent jobs per execution host
        self.max_jobs = conf.ssh_max_jobs
        #: Scheduler name
        self.name = "ssh"
        #: Dictionary of ssh hosts and connection objects
        self.hosts = {}
        #: Lock for SSH scheduler
        self.lock = threading.Lock()

    def submit(self, job):
        """
        Submit a job to execution host via SSH queue. The "pbs.sh" script
        should be already present in the ssh_work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """

        # Select execution host
        _queue = G.SERVICE_STORE[job.status.service].config['queue']
        _user = G.SERVICE_STORE[job.status.service].config['username']
        if 'CIS_SSH_HOST' in job.data.data:
            _queue = job.data.data['CIS_SSH_HOST']

        # Check that maximum job limit is not exceeded
        # Use exclusive session to be thread safe, no need to pass the session
        # from JobManager
        _job_count = -1
        try:
            with Jobs.session_scope(self.state_manager) as _session:
                _job_count = self.state_manager.get_job_count(scheduler=self.name,
                                                             session=_session)
                if _job_count < 0:
                    return False
                # @TODO fix !!! self.max_jobs[_queue], check that queue is defined,
                # otherwise use some default. What happens with the job when submit
                # fails - switch to waiting state? What if someting is mosconfigured
                # and it will never enter queue - max submit retries?
                if _job_count >= self.max_jobs:
                    return False

                # Path names
                _work_dir = os.path.join(self.work_path, job.id())
                _run_script = os.path.join(_work_dir, "pbs.sh")
                _output_log = os.path.join(_work_dir, "output.log")

                # @TODO handle timouts etc ...
                try:
                    # Submit
                    logger.debug("@SSH - Submitting new job: %s@%s", _user, _queue)
                    # Run shsub with proper user permissions
                    _shsub = os.path.join(os.path.dirname(conf.daemon_path_installdir),
                                          "Scripts")
                    _shsub = os.path.join(_shsub, "shsub")
                    _ssh = self.__get_ssh_connection(_queue, _user)
                    _comm = [_shsub, "-i", job.id(), "-d", _work_dir, "-o",
                             _output_log, _run_script]
                    logger.log(VERBOSE, "@SSH - Running command: %s", _comm)
                    # Submit the job. Will rise exception if shsub would return an error
                    _result = _ssh.run(_comm)
                    logger.log(VERBOSE, [_result.output, _result.stderr_output])
                    # Hopefully shsub returned meaningful job ID
                    _ssh_id = _result.output.strip()
                except:
                    job.die("@SSH - Unable to submit job %s." % job.id(), exc_info=True)
                    return False

                # Store the SSH job ID
                job.scheduler = Jobs.SchedulerQueue(scheduler=self.name, id=_ssh_id, queue=_queue)
                # Reduce memory footprint
                job.compact()

                logger.info("Job successfully submitted: %s", job.id())
        except:
            logger.error("@SSH - Unable to connect to DB.", exc_info=True)

        return True

    @rollback(SQLAlchemyError)
    def update(self, jobs):
        """
        Update job states to match their current state on SSH execution host.

        :param jobs: A list of Job instances for jobs to be updated.
        """
        # Extract list of user names and queues associated to the jobs
        _users = {}
        for _job in jobs:
            _service = G.SERVICE_STORE[_job.status.service]
            _usr = _service.config['username']
            if _usr not in _users:
                _users[_usr] = []
            # TODO rewrite to get an array JID -> queue from SchedulerQueue table with single SELECT
            _queue = _job.scheduler.queue
            if _queue not in _users[_usr]:
                _users[_usr].append(_queue)

        # We agregate the jobs by user. This way one shstat call per user is
        # required instead of on call per job
        _job_states = {}
        for _usr, _queues in _users.items():
            for _queue in _queues:
                try:
                    # Run shtat
                    logger.log(VERBOSE, "@SSH - Check jobs state for user %s @ %s",
                               _usr, _queue)
                    _shstat = os.path.join(
                        os.path.dirname(conf.daemon_path_installdir),
                        "Scripts"
                    )
                    _shstat = os.path.join(_shstat, "shstat")
                    _ssh = self.__get_ssh_connection(_queue, _usr)
                    _opts = [_shstat, ]
                    logger.log(VERBOSE, "@SSH - Running command: %s", _opts)
		    # Run shstat. Will rise exception if shstat would return an error
                    _result = _ssh.run(_opts)
                    _output = _result.output
                    logger.log(VERBOSE, [_output, _result.stderr_output])
                except:
                    logger.error("@SSH - Unable to check jobs state.",
                                 exc_info=True)
                    continue

                for _line in _output.splitlines():
                    try:
                        _jid, _state = _line.split(" ")
                        _job_states[_jid] = _state
                    except:
                        logger.error("@SSH - Unable to parse shstat output.",
                                     exc_info=True)
                        return

        # Iterate through jobs
        for _job in jobs:
            # TODO rewrite to get an array JID -> queue from SchedulerQueue table with single SELECT
            _pid = str(_job.scheduler.id)
            logger.log(VERBOSE, "Check job: %s - %s", _job.id(), _job.scheduler.id)
            # Check if the job exists on a SSH execution host
            if _pid not in _job_states:
                _job.die('@SSH - Job %s does not exist on any of the SSH '
                         'execution hosts' % _job.id())
            else:
                # Update job progress output
                self.progress(_job)
                _state = int(_job_states[_pid])
                # Job has finished. Check the exit code.
                if _state >= 0:
                    _new_state = 'done'
                    _msg = 'Job finished succesfully'
                    if _state > 128:
                        _new_state = 'killed'
                        _msg = 'Job was killed by a signal'
                    elif _state > 0:
                        _new_state = 'failed'
                        _msg = 'Job finished with error code'
                    try:
                        _job.finish(_msg, _new_state, _state)
                    except:
                        _job.die('@SSH - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id()), exc_info=True)
                # Job is running
                elif _state == -1:
                    if _job.get_state() != 'running':
                        try:
                            _job.run()
                        except:
                            _job.die("@SSH - Unable to set job state "
                                     "(running : %s)" % _job.id(), exc_info=True)
                # Treat all other states as queued
                else:
                    _new_state = 'failed'
                    _msg = 'Job finished with unknown exit state'
                    try:
                        _job.finish(_msg, _new_state, _state)
                    except:
                        _job.die('@SSH - Unable to set job state (%s : %s)' %
                                 (_new_state, _job.id()), exc_info=True)

    def stop(self, job, msg, exit_code):
        """
        Stop running job and remove it from SSH queue.

        :param job: :py:class:`Job` instance
        :param msg: Message that will be passed to the user
        """
        _pid = ''

        # Get Job PID
        try:
            _pid = str(job.scheduler.id)
        except:
            job.die('@SSH - Unable to read PID', exc_info=True)
            return

        # Run shdel
        logger.debug("@SSH - Killing job")
        # Run shdel with proper user permissions
        _usr = G.SERVICE_STORE[job.status.service].config['username']
        _queue = job.scheduler.queue
        _shdel = os.path.join(
            os.path.dirname(conf.daemon_path_installdir),
            "Scripts"
        )
        _shdel = os.path.join(_shdel, "shdel")
        _ssh = self.__get_ssh_connection(_queue, _usr)
        _opts = [_shdel, _pid]
        logger.log(VERBOSE, "@SSH - Running command: %s", _opts)
        _result = _ssh.run(_opts, allow_error=True)
        logger.log(VERBOSE, [_result.output, _result.stderr_output])
        # Check return code. If == 1 the job has already finished and we do not raise an exception
        if _result.return_code == 1:
            logger.debug("Job %s already finished.", job.id())
            return
        if _result.return_code != 0:
            raise OSError((
                _result.return_code,
                "shdel returned non zero exit code.\n%s" %
                str([_result.output, _result.stderr_output])
            ))

        # Mark as killed by user
        job.mark(msg, exit_code)

    def __get_ssh_connection(self, host_name, user_name):
        _login = "%s@%s" % (user_name, host_name)
        self.lock.acquire()
        if _login in self.hosts:
            self.lock.release()
            return self.hosts[_login]
        else:
            try:
                logger.log(VERBOSE, "Create new SSH connection.")
                try:
                    _ssh = spur.SshShell(  # Assume default private key is valid
                                           hostname=host_name,
                                           username=user_name,
                                           # Workaround for paramiko not understanding host ecdsa keys.
                                           # @TODO If possible disable such keys on sshd at host and
                                           # remove this from production code.
                                           missing_host_key=spur.ssh.MissingHostKey.accept,
                                           # Hack to speedup connection setup - use a minimal known_hosts file - requires a patch for spur
                                           # Patch in Scripts directory
                                           known_hosts_file=conf.ssh_known_hosts
                    )
                except TypeError:  # Fallback when spur is not patched
                    _ssh = spur.SshShell(  # Assume default private key is valid
                                           hostname=host_name,
                                           username=user_name,
                                           # Workaround for paramiko not understanding host ecdsa keys.
                                           # @TODO If possible disable such keys on sshd at host and
                                           # remove this from production code.
                                           missing_host_key=spur.ssh.MissingHostKey.accept
                    )
                self.hosts[_login] = _ssh
                # Establish the connection
                _comm = ["/bin/hostname", ]
                _ssh.run(_comm)
            finally:
                logger.log(VERBOSE, "SSH connection ready.")
                self.lock.release()
            return _ssh


class DummyScheduler(Scheduler):
    """
    Class implementing dummy scheduler. A scheduler that does not submit jubs
    only lies that it did.

    Allows for job submission, deletion and extraction of job status.
    """

    def __init__(self):
        super(DummyScheduler, self).__init__()
        #: Dict of maximum number of concurrent jobs per execution host
        self.max_jobs = conf.dummy_max_jobs
        #: Scheduler name
        self.name = "dummy"

    def submit(self, job):
        """
        Submit a job to execution host via SSH queue. The "pbs.sh" script
        should be already present in the ssh_work_path/job directory.

        :param job: :py:class:`Job` instance
        :return: True on success and False otherwise.
        """
        #@TODO Rewrite submit, chain_jobs, generate_scripts to throw exceptions on errors
        logger.log(VERBOSE, "Trying to submit job %s", job.id())
        # Check that maximum job limit is not exceeded
        # Use exclusive session to be thread safe, no need to pass the session
        # from JobManager
        _job_count = -1
        try:
            with Jobs.session_scope(self.state_manager) as _session:
                _job_count = self.state_manager.get_job_count(scheduler=self.name,
                                                             session=_session)
                if _job_count < 0:
                    return False
                # @TODO fix !!! self.max_jobs[_queue], check that queue is defined,
                # otherwise use some default. What happens with the job when submit
                # fails - switch to waiting state? What if someting is mosconfigured
                # and it will never enter queue - max submit retries?
                if _job_count >= self.max_jobs:
                    logger.debug("Active scheduler jobs limit reached - job will be held")
                    return False

                # Store the SSH job ID
                job.scheduler = Jobs.SchedulerQueue(scheduler=self.name, id=0, queue="default")
                # Reduce memory footprint
                job.compact()

                logger.info("Job successfully submitted: %s", job.id())
        except:
            logger.error("Unable to connect to DB.", exc_info=True)

        return True

    @rollback(SQLAlchemyError)
    def update(self, jobs):
        """
        Update job states to match their current state. Sets jobs state randomly.

        :param jobs: A list of Job instances for jobs to be updated.
        """
        # Iterate through jobs
        for _job in jobs:
            self.progress(_job)
            if conf.dummy_turbo:
                _state = 2
            else:
                _state = random.randint(0,2)
            if _state == 1:
                # Job is running
                if _job.get_state() != 'running':
                    try:
                        _job.run()
                    except:
                        _job.die("Unable to set job state "
                                 "(running : %s)" % _job.id(), exc_info=True)
            elif _state == 2:
                _new_state = 'done'
                _msg = 'Job finished succesfully'
                try:
                    _job.finish(_msg, _new_state, _state)
                except:
                    _job.die('Unable to set job state (%s : %s)' %
                             (_new_state, _job.id()), exc_info=True)

    def stop(self, job, msg, exit_code):
        """
        Stop running job and remove it from SSH queue.

        :param job: :py:class:`Job` instance
        :param msg: Message that will be passed to the user
        """
        # Mark as killed by user
        job.mark(msg, exit_code)

    def progress(self, job):
        """
        Extract the job progress log and expose it to the user.

        :param job: :py:class:`Job` instance
        """
        pass

    def finalise(self, job):
        """
        Prepare output of finished job.

        Job working directory is moved to the external_data_path directory.

        :param job: :py:class:`Job` instance
        """
        _jid = job.id()
        # Remove job from scheduler queue if it was queued
        try:
            job.scheduler = None
        except:
            logger.error("@Scheduler - Unable to remove job SchedulerQueue.",
                         exc_info=True)

        try:
            # Set job exit state
            job.exit()
        except:
            logger.error("@Scheduler - Unable to finalize cleanup.",
                         exc_info=True)
        logger.info("Job finalised: %s", _jid)

    def abort(self, job):
        """
        Cleanup aborted job.

        Removes working and output directories.

        :param job: :py:class:`Job` instance
        """
        _jid = job.id()
        # Remove job from scheduler queue if it was queued
        try:
            job.scheduler = None
        except:
            logger.error("@Scheduler - Unable to remove job SchedulerQueue.",
                         exc_info=True)

        logger.debug("@Scheduler - finalize cleanup: %s", _jid)
        try:
            # Set job exit state
            job.exit()
        except:
            logger.error("@Scheduler - Unable to finalize cleanup.",
                         exc_info=True)
        logger.debug("@Scheduler - job abort finished: %s", _jid)

    def generate_scripts(self, job):
        """
        Generate scripts and job input files from templates.

        Will walk through service_data_path/service and copy all files
        including recursion through subdirectories to PBS work directory for
        specified job (the directory structure is retained). For all files
        substitute all occurences of @@{atribute_name} with specified values.

        :param job: :py:class:`Job` instance after validation
        :return: True on success and False otherwise.
        """
        logger.debug('Generate sripts')
        return True

    def chain_input_data(self, job):
        """
        Chain output data of finished jobs as our input.

        Copies output data of specified job IDs into the working directory of
        current job.

        :param job: :py:class:`Job` instance after validation
        :return: True on success and False otherwise.
        """
        logger.debug('Chaining input data')
        return True


class SchedulerStore(dict):
    def __init__(self):
        super(SchedulerStore, self).__init__()

    def init(self):
        # Load all files from service_conf_path. Configuration files should be
        # in JSON format.
        logger.debug('Initializing schedulers.')

        for _scheduler in conf.service_schedulers:
            if _scheduler == 'pbs':
                self[_scheduler] = PbsScheduler()
            elif _scheduler == 'ssh':
                self[_scheduler] = SshScheduler()
            elif _scheduler == 'dummy':
                self[_scheduler] = DummyScheduler()

