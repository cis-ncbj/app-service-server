# -*- coding: utf-8 -*-

# Based on runner.py from python-daemon, an implementation of PEP 3143.
#
# Copyright © 2013 Konrad Klimaszewski <konrad.klimaszewski@ncbj.gov.pl>
# Copyright © 2009–2010 Ben Finney <ben+python@benfinney.id.au>
# Copyright © 2007–2008 Robert Niederreiter, Jens Klein
# Copyright © 2003 Clark Evans
# Copyright © 2002 Noah Spurrier
# Copyright © 2001 Jürgen Hermann
#
# This is free software: you may copy, modify, and/or distribute this work
# under the terms of the Python Software Foundation License, version 2 or
# later as published by the Python Software Foundation.
# No warranty expressed or implied. See the file LICENSE.PSF-2 for details.

"""
Implementation of daemon mode for CISAppServer.
"""

from __future__ import absolute_import

import os
import sys
import signal
import errno
import argparse
import logging
import logging.config
import lockfile
import time

from daemon import pidfile, DaemonContext

from logging import \
    debug, info

from .JobManager import JobManager, version
from .Config import conf
from . import Tools as T

logger = logging.getLogger(__name__)


class DaemonRunnerError(Exception):
    """ Abstract base class for errors from DaemonRunner. """


class DaemonRunnerInvalidActionError(ValueError, DaemonRunnerError):
    """ Raised when specified action for DaemonRunner is invalid. """


class DaemonRunnerStartFailureError(RuntimeError, DaemonRunnerError):
    """ Raised when failure starting DaemonRunner. """


class DaemonRunnerStopFailureError(RuntimeError, DaemonRunnerError):
    """ Raised when failure stopping DaemonRunner. """


class DaemonRunnerReloadFailureError(RuntimeError, DaemonRunnerError):
    """ Raised when failure reloading DaemonRunner. """


class DaemonRunner(object):
    """
    Controller for JobManager instance running in a separate background
    process.

    The command-line argument is the action to take:

    * 'start': Become a daemon and call `app.run()`.
    * 'stop': Exit the daemon process specified in the PID file.
    * 'terminate' : Exit the daemon process specified in the PID file, kill all
                    active jobs
    * 'restart': Stop, then start.
    * 'reload': Reread the configuration file.
    * 'pause' : Stop the job queue
    * 'run' : Restart the job queue

    """

    def __init__(self):
        """
        Upon initialization parses command line arguments, loads options
        from input files. Initializes Validator and Sheduler interfaces.
        """
        self.parse_args()

        if self.action in ['start', 'stop', 'restart']:
            logging.config.dictConfig(conf.log_config)

        self.job_manager = None
        self.daemon_context = DaemonContext()

        # Prepare PID file
        self.pidfile = None
        if conf.daemon_path_pidfile is not None:
            self.pidfile = make_pidlockfile(
                conf.daemon_path_pidfile, conf.daemon_pidfile_timeout)
        self.daemon_context.pidfile = self.pidfile

        # Set work path (by default daemon will endup in /)
        self.daemon_context.working_directory = conf.daemon_path_workdir

        # Do not close stdout and stderr in case opening daemon context throws
        # an exception that we want to report to console
        self.daemon_context.stdout = sys.stdout
        self.daemon_context.stderr = sys.stderr

        # Set signal handlers - they will react to action requests by user
        self.daemon_context.signal_map = {
            signal.SIGTERM: self.cleanup,
            signal.SIGHUP: self.reload_config,
            signal.SIGUSR1: self.pause,
            signal.SIGUSR2: self.unpause,
        }

        # Protect log file from closing
        for _log in logging.root.handlers:
            if isinstance(_log, logging.StreamHandler) or \
               isinstance(_log, logging.handlers.RotatingFileHandler):
                self.daemon_context.files_preserve = [_log.stream.fileno()]

    def parse_args(self, argv=None):
        """ Parse command-line arguments. """
        # Options parser
        _desc = 'Daemon responsible for handling CIS Web Apps Requests.'
        _parser = argparse.ArgumentParser(description=_desc)
        _parser.add_argument(
            '-c', '--config', dest='config', action='store',
            default='CISAppServer.json', help='Configuration file.')
        _parser.add_argument(
            '--log', dest='log', action='store', default='DEBUG',
            choices=['VERBOSE', 'DEBUG', 'INFO', 'WARNING', 'ERROR'],
            help='Logging level.')
        _parser.add_argument(
            '--log-output', dest='log_output', action='store',
            help='Store the logs into LOG-OUTPUT.')
        _parser.add_argument(
            'action', action='store',
            choices=[
                'start', 'stop', 'terminate', 'restart', 'reload', 'pause',
                'run'
            ],
            help='ACTION to be performed:\n'
            'start - start new daemon instance\n'
            'stop - stop deamon\n'
            'terminate - stop daemon and kill all running jobsn\n'
            'restart - stop current and start new daemon instance\n'
            'reload - reread configuration file\n'
            'pause - stop accepting new jobs\n'
            'run - restart accepting new jobs\n'
        )
        _args = _parser.parse_args()

        # Setup logging interface
        conf.log_level_cli = _args.log.upper()  #: Logging level to use
        conf.log_output_cli = _args.log_output  #: Log output file name
        logging.addLevelName(T.VERBOSE, 'VERBOSE')
        _log_level = getattr(logging, conf.log_level_cli)
        logging.basicConfig(
            level=_log_level,
            format='%(levelname)-8s %(message)s',
        )

        info("CISAppS %s" % version)
        info("Logging level: %s" % conf.log_level)
        info("Configuration file: %s" % _args.config)

        # Load configuration from option file
        debug('@Daemon - Loading global configuration ...')
        conf.load(_args.config)

        self.action = unicode(_args.action)
        if self.action not in self.action_funcs:
            raise DaemonRunnerInvalidActionError(
                "Unexpected action requested: %s" % self.action)

    def _start(self):
        """ Open the daemon context and run JobManager. """
        if is_pidfile_stale(self.pidfile):
            self.pidfile.break_lock()

        # Instatiate job_manager
        self.job_manager = JobManager()

        # Start daemon context
        try:
            self.daemon_context.open()
        except lockfile.AlreadyLocked:
            # TODO output somehow to the console?
            raise DaemonRunnerStartFailureError(
                u"PID file %r already locked" % self.pidfile.path)

        # Catch exceptions and log them otherwise we will not see what happened
        try:
            pid = os.getpid()
            # info("CISAppServer started with pid %d" % pid)
            logger.info("CISAppServer started with pid %d" % pid)
            # Disable console logging
            _h = logging.root.handlers[0]
            logging.root.removeHandler(_h)

            self.job_manager.run()
        except Exception, e:
            logger.error(u"@Daemon - Shutdown, exception cought: %s" % e)
            logging.shutdown()
            sys.exit(1)

    def _terminate_daemon_process(self):
        """
        Terminate the daemon process specified in the current PID file.
        Daemon will kill all active jobs.
        """
        self._signal_daemon_process(signal.SIGTERM)

    def _kill_daemon_process(self):
        """
        Terminate the daemon process specified in the current PID file.
        Daemon will not touch active jobs.
        """
        self._signal_daemon_process(signal.SIGKILL)

    def _signal_daemon_process(self, user_signal):
        """
        Send signal to the daemon process specified in the current PID file.
        """
        pid = self.pidfile.read_pid()
        try:
            os.kill(pid, user_signal)
        except OSError, exc:
            raise DaemonRunnerStopFailureError(
                u"Failed to send signal %d to PID %d: %s" %
                (user_signal, pid, exc)
            )

    def _stop(self):
        """
        Exit the daemon process specified in the current PID file.
        """
        if not self.pidfile.is_locked():
            raise DaemonRunnerStopFailureError(
                u"PID file %r not locked" % self.pidfile.path)

        if is_pidfile_stale(self.pidfile):
            self.pidfile.break_lock()
        else:
            self._kill_daemon_process()

    def _terminate(self):
        """
        Exit the daemon process specified in the current PID file.
        """
        if not self.pidfile.is_locked():
            raise DaemonRunnerStopFailureError(
                u"PID file %r not locked" % self.pidfile.path)

        if is_pidfile_stale(self.pidfile):
            self.pidfile.break_lock()
        else:
            self._terminate_daemon_process()

    def _restart(self):
        """ Stop, then start. """
        self._stop()
        time.sleep(1)
        self._start()

    def _reload(self):
        """ Signal running daemon to reload configuration. """
        if not self.pidfile.is_locked():
            raise DaemonRunnerReloadFailureError(
                u"PID file %r not locked" % self.pidfile.path)

        if is_pidfile_stale(self.pidfile):
            self.pidfile.break_lock()
            raise DaemonRunnerReloadFailureError(
                u"PID file %r is stale" % self.pidfile.path)
        else:
            self._signal_daemon_process(signal.SIGHUP)

    def _pause(self):
        """ Signal running daemon to pause job queue. """
        if not self.pidfile.is_locked():
            raise DaemonRunnerReloadFailureError(
                u"PID file %r not locked" % self.pidfile.path)

        if is_pidfile_stale(self.pidfile):
            self.pidfile.break_lock()
            raise DaemonRunnerReloadFailureError(
                u"PID file %r is stale" % self.pidfile.path)
        else:
            self._signal_daemon_process(signal.SIGUSR1)

    def _run(self):
        """ Signal running daemon to restart job queue. """
        if not self.pidfile.is_locked():
            raise DaemonRunnerReloadFailureError(
                u"PID file %r not locked" % self.pidfile.path)

        if is_pidfile_stale(self.pidfile):
            self.pidfile.break_lock()
            raise DaemonRunnerReloadFailureError(
                u"PID file %r is stale" % self.pidfile.path)
        else:
            self._signal_daemon_process(signal.SIGUSR2)

    action_funcs = {
        u'start': _start,
        u'stop': _stop,
        u'terminate': _terminate,
        u'restart': _restart,
        u'reload': _reload,
        u'pause': _pause,
        u'run': _run,
    }

    def _get_action_func(self):
        """
        Return the function for the specified action.

        Raises ``DaemonRunnerInvalidActionError`` if the action is
        unknown.
        """
        try:
            func = self.action_funcs[self.action]
        except KeyError:
            raise DaemonRunnerInvalidActionError(
                u"Unknown action: %r" % self.action)
        return func

    def cleanup(self, signum, frame):
        logger.info("Received terminate command")
        self.job_manager.shutdown()
        logger.info("Shutdown")
        logging.shutdown()
        sys.exit(0)

    def reload_config(self, signum, frame):
        logger.info("Received reload command")
        logger.debug("PWD: %s" % os.getcwd())
        conf.load(conf.config_file)
        self.job_manager.init()
        logger.info("Reload complete")

    def pause(self, signum, frame):
        logger.info("Received pause command")
        self.job_manager.stop()

    def unpause(self, signum, frame):
        logger.info("Received run command")
        self.job_manager.start()

    def run(self):
        """ Perform the requested action. """
        logger.info("Executing %s" % self.action)
        func = self._get_action_func()
        func(self)


def make_pidlockfile(path, acquire_timeout):
    """ Make a PIDLockFile instance with the given filesystem path. """
    if not isinstance(path, basestring):
        err = ValueError(u"Not a filesystem path: %r" % path)
        raise err
    if not os.path.isabs(path):
        err = ValueError(u"Not an absolute path: %r" % path)
        raise err
    lockfile = pidfile.TimeoutPIDLockFile(path, acquire_timeout)

    return lockfile


def is_pidfile_stale(pidfile):
    """
    Determine whether a PID file is stale.

    Return ``True`` (“stale”) if the contents of the PID file are
    valid but do not match the PID of a currently-running process;
    otherwise return ``False``.

    """
    result = False

    pidfile_pid = pidfile.read_pid()
    if pidfile_pid is not None:
        try:
            os.kill(pidfile_pid, signal.SIG_DFL)
        except OSError, exc:
            if exc.errno == errno.ESRCH:
                # The specified PID does not exist
                result = True

    return result


if __name__ == "__main__":
    daemon = DaemonRunner()
    daemon.run()
