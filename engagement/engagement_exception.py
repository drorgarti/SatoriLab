class EngagementException(Exception):

    def __init__(self, value, fatal=False):
        self.value = value
        self.fatal = fatal

    def __str__(self):
        return repr('%s (fatal=%s)' % (self.value, self.fatal))

    def is_fatal(self):
        return self.fatal
