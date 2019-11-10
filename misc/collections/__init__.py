def is_list_alike(obj):
    """ Check if data was iterable but not a string.

    see: http://stackoverflow.com/a/17222092
    """
    try:
        iter(obj)
    except TypeError:
        return False

    try:
        if isinstance(obj, basestring):
            return False
    except NameError:
        if isinstance(obj, str):
            return False

    if isinstance(obj, bytes):
        return False

    if isinstance(obj, dict):
        return False

    return True
