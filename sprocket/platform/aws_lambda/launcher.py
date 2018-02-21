#!/usr/bin/python

from sprocket.platform.launcher import LauncherBase
import pylaunch


class Launcher(LauncherBase):
    """
    AWS Lambda launcher
    """

    @classmethod
    def initialize(cls, launch_queue):
        """
        A blocking call to initialize the launcher.
        :param launch_queue: the event queue through which future launch events will be sent
        :return: normally does not return
        """
        pylaunch.initialize_launch()
        while True:
            launch_ev = launch_queue.get()
            pylaunch.launchpar_async(launch_ev.nlaunch, launch_ev.fn_name, launch_ev.akid, launch_ev.secret,
                               launch_ev.payload,
                               launch_ev.regions)
