import sys


class ErrorHandling(object):
    def __init__(self):
        pass

    @staticmethod
    def warn(string):
        """

        :param string:
        """
        print >> sys.stderr, string