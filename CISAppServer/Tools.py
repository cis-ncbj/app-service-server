#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Utility classes used by CISAppServer.
"""

import os
import logging
from decorator import decorator

import Globals as G
from Config import conf, VERBOSE, ExitCodes


logger = logging.getLogger(__name__)


def rollback(exception_to_check):
    """
    Rollback decorator for StateManager DB calls

    param: exception_to_check - Exception class or list of classes for which rollback will be issued
    """
    def rollback(f, *args, **kw):
        try:
            return f(*args, **kw)
        except exception_to_check as e:
            session = G.STATE_MANAGER.session
            # Check that the function we decorate was not called with custom session. If yes use it.
            if "session" in kw:
                session = kw["session"]
            logger.log(VERBOSE, "Rollback DB session")
            # Rollback the session and reraise the exception so it will be properly handled
            session.rollback()
            raise
    # Using decorator module will preserve signature of decorated function
    return decorator(rollback)

