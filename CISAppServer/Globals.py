# -*- coding: UTF-8 -*-
"""
Module with global instances

This is actually a hack to overcome circular dependancies.
Most elements of the system were envisioned as singletons. However proper
implementation in python is ineficient (to many costly calls). A trick used to
shadow the class with it's instance breakes IDEs and Sphinx.

Therefore they are implemented as module globals for now.

After refactoring this could go away.
"""

#STATE_MANAGER = None
VALIDATOR = None
SERVICE_STORE = None
SCHEDULER_STORE = None


def init():
    """
    Initialize the global instances.
    """
#    import Jobs
    import Schedulers
    import Services

#    global STATE_MANAGER
    global VALIDATOR
    global SERVICE_STORE
    global SCHEDULER_STORE
#    STATE_MANAGER = Jobs.FileStateManager()
    VALIDATOR = Services.Validator()
    SERVICE_STORE = Services.ServiceStore()
    SCHEDULER_STORE = Schedulers.SchedulerStore()

#    STATE_MANAGER.init()
    SCHEDULER_STORE.init()
    SERVICE_STORE.init()
