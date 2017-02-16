import os
import logging
import re
import asyncio.log

stats = None # global var / singleton for slow asyncio stats

class SlowLogHandler(logging.Handler):
    """ a small debug logger that shall capture 
        slow asyncio processes.
        and allow to create reports
    """
    def __init__(self):
        super().__init__()
        self.log_trace = []
        self.slow_rex = re.compile(r'.*Executing.*took\s.*\sseconds')

    def emit(self, record):
        msg = record.msg
        if self.slow_rex.match(msg):
            self.log_trace.append((record.msg % record.args, record.args[1])) 

    def num_entries(self):
        return len(self.log_trace)

    def show_entries(self):
        print("Got %d entries" % len(self.log_trace))
        for idx, entry in enumerate(self.log_trace):
            print("%2d %r %r" % ((idx,) + entry))


class SlowAsyncIOStats:
    def __init__(self):
        """ sets up collection of stats for slow asyncio tasks

        """
        # set env var in order to allow reporting of slow tasks
        self.enable_slow_logs()
        
        # create a custom logger and attach it to the asyncio logging
        self.logger = logging.getLogger(asyncio.log.__package__)
        self.slow_handler = SlowLogHandler()
        self.logger.handlers.append(self.slow_handler)

    @staticmethod
    def enable_slow_logs():
        """ enables logging of slow events """
        os.environ['PYTHONASYNCIODEBUG'] = '1'

    @staticmethod
    def disable_slow_logs():
        """ disables logging of slow events """
        os.environ['PYTHONASYNCIODEBUG'] = '0'

    def get_stats(self):
        rslt = []
        slow_handler = self.slow_handler

    def show_entries(self):
        self.slow_handler.show_entries()



def enable_slow_log_stats():
    """ enables asyncio stats for slow execution 
    """

    global stats
    stats = SlowAsyncIOStats()


def show_slow_log_stats():
    if stats:
        stats.show_entries()



