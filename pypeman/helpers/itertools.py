def flatten(items):
    """ Yield items from any nested iterable. """
    for x in items:
        if isinstance(x, (list, tuple)) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x
