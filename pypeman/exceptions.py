class EndChanProcess(StopAsyncIteration):
    """
    A custom excption to tell that the channel reached the end
    Currently, only used by conditional sub channel to avoid
    calling nodes after it
    """


class Dropped(Exception):
    """ Used to stop process as message is processed. Default success should be returned.
    """


class Rejected(Exception):
    """ Used to tell caller the message is invalid with an error return.
    """


class ChannelStopped(Exception):
    """ The channel is stopped and can't process message.
    """


class RetryException(Exception):
    """
        Custom Exception that is raise when a pypeman node catch an exception
        that should trigger a delayed relaunch of the msg.
    """


class PausedChanException(Exception):
    """
        Custom Exception that is raise when a pypeman channel is paused
    """
