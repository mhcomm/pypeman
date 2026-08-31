from functools import wraps


def awaitify(sync_func):
    """
    Wrap a synchronous callable to allow ``await``'in it
    (asyncio.coroutine was removed in python 3.11)
    TODO: Check if there's a proper way to rewrite all sync
    funcs (that calls Event)into native coroutines
    """
    @wraps(sync_func)
    async def async_func(*args, **kwargs):
        return sync_func(*args, **kwargs)
    return async_func
