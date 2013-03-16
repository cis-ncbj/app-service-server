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

from CISAppServer.Tools import Validator, PbsManager, Tools

version = "0.1"

class JobManager(Tools):
    """Main calss of CISAppServer. It is responsible for job management."""

    #TODO daemon mode
    #TODO on the fly configuration reload
    #TODO queue stop (do not accept new jobs)
    #TODO hide file operations under some common API ???
    #TODO compactify the error, warning message generation ???
    def __init__(self):
        """Ctor for JobManager. Parses command line srguments, loads options
        from input files. Initializes Validator and PBS interface."""

        # Options parser
        _parser = argparse.ArgumentParser(
                description='Daemon responsible for handling CIS Web Apps Requests.'
                )
        _parser.add_argument('-c', '--config', dest='config', action='store',
                default='CISAppServer.json',
                help='Configuration file.')
        _parser.add_argument('--log', dest='log', action='store',
                default='DEBUG',
                help='Logging level: DEBUG, INFO, WARNING, ERROR.')
        _parser.add_argument('--log-output', dest='log_output', action='store',
                help='Store the logs into LOG-OUTPUT.')
        _args = _parser.parse_args()

        # Configuration of CISAppServer
        # Define default values
        self.config = {
                'config_file' : _args.config,
                'log_level' : _args.log.upper(),
                'log_output' : _args.log_output,
                'service_conf_path' : 'Services',
                'service_data_path' : 'Services/Data',
                'external_queue_path' : 'Queue',
                'external_data_path' : 'Output',
                'run_interval' : 1,
                'reserved_keys' : ('service', 'name'),
                'pbs_queue_path' : 'PBS/Queue',
                'pbs_work_path' : 'PBS/Scratch',
                'pbs_queue' : 'default',
                'pbs_max_jobs' : 100
                }

        # Setup logging interface
        _log_level = getattr(logging, self.config['log_level'])
        if self.config['log_output']:
            logging.basicConfig(level=_log_level,
                    filename=self.config['log_output'])
        else:
            logging.basicConfig(level=_log_level)

        info("CISAppS %s"%version)
        info("Logging level: %s"%self.config['log_level'])
        info("Configuration file: %s"%self.config['config_file'])

        # Load configuration from option file
        debug('Loading global configuration ...')
        _conf_file = open(self.config['config_file'])
        _conf = json.load(_conf_file)
        _conf_file.close()
        debug(json.dumps(_conf))
        self.config.update(_conf)

        # Normalize paths to full versions
        for _key in self.config.keys():
            if _key.endswith('path'):
                self.config[_key] = os.path.realpath(self.config[_key])

        # Shortcut variables
        self.jobs_dir = os.path.join(self.config['external_queue_path'], 'jobs')
        self.queue_dir = os.path.join(self.config['external_queue_path'], 'queue')
        self.done_dir = os.path.join(self.config['external_queue_path'], 'done')
        self.running_dir = os.path.join(self.config['external_queue_path'], 'running')
        self.delete_dir = os.path.join(self.config['external_queue_path'], 'delete')

        # Initialize Validator and PbsManager
        self.validator = Validator(self.config)
        self.pbs = PbsManager(self.config)


    def check_new_jobs(self):
        """Check for new job files in the queue directory. If found try to
        submit them to PBS."""

        # New jobs are put into the "queue" directory
        _queue = os.listdir(self.queue_dir)
        for _job in _queue:
            debug('Detected new job: %s'%_job)
            try:
                # Move the symlink from "queue" to "running" or done if
                # sumbission failed
                os.unlink(os.path.join(self.queue_dir, _job))
                if self.submit(_job):
                    os.symlink(
                            os.path.join(self.jobs_dir, _job),
                            os.path.join(self.running_dir, _job)
                            )
                else:
                    os.symlink(
                            os.path.join(self.jobs_dir, _job),
                            os.path.join(self.done_dir, _job)
                            )
            except OSError:
                error("Cannot start job", exc_info=True)


    def check_running_jobs(self):
        """Check status of running jobs. If finished jobs are found perform
        finalisation."""

        # Check the "pbs_queue_path" for files with PBS job IDs. This directory
        # should only be accessible inside of the firewall so it should be safe
        # from corruption by clients.
        _queue = os.listdir(self.config['pbs_queue_path'])
        for _job in _queue:
            if self.pbs.status(_job) == 'done':
                # Found finished job - finalise it
                debug('Detected finished job: %s'%_job)
                self.pbs.finalise(_job)
                try:
                    os.unlink(os.path.join(self.running_dir, _job))
                    os.symlink(
                            os.path.join(self.jobs_dir, _job),
                            os.path.join(self.done_dir, _job)
                            )
                except OSError:
                    error("Cannot finalise job", exc_info=True)


    def check_deleted_jobs(self):
        """Check for jobs marked for removal. If found remove all files related
        to the job."""

        # Symlinks in "delete" dir mark jobs for removal
        _queue = os.listdir(self.delete_dir)
        for _job in _queue:
            debug('Detected job marked for deletion: %s'%_job)
            try:
                # Remove job file and its symlinks
                os.unlink(os.path.join(self.jobs_dir, _job))
                os.unlink(os.path.join(self.delete_dir, _job))
                if os.path.exists(os.path.join(self.queue_dir, _job)):
                    os.unlink(os.path.join(self.queue_dir, _job))
                if os.path.exists(os.path.join(self.running_dir, _job)):
                    self.pbs.stop(_job)
                    os.unlink(os.path.join(self.running_dir, _job))
                if os.path.exists(os.path.join(self.done_dir, _job)):
                    os.unlink(os.path.join(self.done_dir, _job))
            except OSError:
                error("Cannot remove job", exc_info=True)

            _output = os.path.join(self.config['external_data_path'], _job)
            if os.path.isdir(_output):
                shutil.rmtree(_output, onerror=self._rmtree_error)
            #TODO add cleanup of jobs that were interrupted before their output
            #was retrieved


    def check_old_jobs(self):
        """Check for jobs that exceed their life time. If found mark them for
        removal. [Not Implemented]"""
        pass


    def submit(self, job):
        """Generate job related scripts and submit them to PBS"""

        if self.validator.prepare_submission(job):
            return self.pbs.submit(job)

        return False


    def run(self):
        """Main loop of JobManager.
        - Check for new jobs - submit them if found,
        - Check for finished jobs - retrive output if found,
        - Check for jobs exceeding their life time - mark for removal,
        - Check for jobs to be removed - delete all related files."""

        while(1):
            time.sleep(self.config['run_interval'])
            self.check_new_jobs()
            self.check_running_jobs()
            self.check_old_jobs()
            self.check_deleted_jobs()


if __name__ == "__main__":
    apps = JobManager()
    apps.run()
