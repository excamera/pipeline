#!/usr/bin/python


class LaunchEvent(object):
    """
    The launch parameters
    """

    def __init__(self, **kwargs):
        self.nlaunch = kwargs.get('nlaunch')
        self.fn_name = kwargs.get('fn_name')
        self.akid = kwargs.get('akid')
        self.secret = kwargs.get('secret')
        self.payload = kwargs.get('payload')
        self.regions = kwargs.get('regions')


class LauncherBase(object):
    """
    The launcher
    """

    @classmethod
    def initialize(cls, launch_queue):
        """
        A blocking call to initialize the launcher.
        :param launch_queue: the event queue through which future launch events will be sent
        :return: normally does not return
        """
        raise NotImplementedError('initialize')
