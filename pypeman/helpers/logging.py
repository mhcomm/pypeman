""" helpers for logging
"""

import contextvars
import logging

# Hold the message currently being processed and the channel processing it.
# Set/reset by BaseChannel.handle() and BaseChannel.inject() so that every
# log record emitted inside the per-message task tree can be correlated.
MSG_CTXVAR = contextvars.ContextVar("pypeman_msg", default=None)
CHANNEL_CTXVAR = contextvars.ContextVar("pypeman_channel", default=None)


class LogContextFilter(logging.Filter):
    """
    Injects message processing context in all log records so formatters
    can use:

    - ``%(msg_id)s``: ``"[<msg uuid>] "`` while a message is being
      processed, empty string otherwise.
    - ``%(channel)s``: ``"(<channel short_name>) "`` while a message is
      being processed, except on records already emitted through the
      channel's own logger (their ``%(name)s`` carries it), empty string
      otherwise.

    Attach it to handlers (not loggers) so records from project or
    third-party code also get the attributes.
    """
    def filter(self, record):
        msg = MSG_CTXVAR.get()
        uuid = getattr(msg, "uuid", None)
        record.msg_id = "[%s] " % uuid if uuid else ""
        chan = CHANNEL_CTXVAR.get()
        if chan is not None and record.name != chan.logger.name:
            record.channel = "(%s) " % chan.short_name
        else:
            record.channel = ""
        return True


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
