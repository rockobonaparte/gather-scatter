__author__ = "Adam Preble"
__copyright__ = "Copyright 2016, Adam Preble"
__credits__ = ["Adam Preble"]
__license__ = "personal"
__version__ = "1.0.0"
__maintainer__ = "Adam Preble"
__email__ = "adam.preble@gmail.com"
__status__ = "Demonstration"


class AgentWhitelist(object):
    """
    A basic whitelist for the Gatherer to track which monitors have connected and which ones we might care about.
    """
    def __init__(self, starter_whitelist=[]):
        self.whitelist = starter_whitelist
        self.reported = []

    def add_necessary(self, necessary):
        if not necessary in self.whitelist:
            self.whitelist.append(necessary)

    def clear_whitelist(self):
        self.whitelist = []

    def reset_reported(self):
        self.reported = []

    def all_reported(self):
        for needed in self.whitelist:
            if not needed in self.reported:
                return False
        return True

    def add_reported(self, reported):
        if not reported in self.reported:
            self.reported.append(reported)
