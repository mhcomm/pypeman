""" helpers for logging 
"""

import logging
log_trace = []

class DebugLogHandler(logging.Handler):
    """ a small debug logger just storing all records in a list """
    def __init__(self):
        super().__init__()
        self.log_trace = []

    def emit(self, record):
        self.log_trace.append(record)


    def num_entries(self):
        return len(self.log_trace)

    def show_entries(self):
        print("Got %d entries" % len(self.log_trace))
        for idx, rec in enumerate(self.log_trace):
            print("%2d %8.3f %s:%d %r" % (idx, rec.relativeCreated/1000., 
                rec.pathname,  rec.lineno,
                rec.message))
