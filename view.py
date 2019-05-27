import os


def init_view():
    """
    Parses the "VIEW" env var and returns the resulting addresses in a list.
    Returns an empty list of the env var is not set or not parsed correctly.

    Returns:
        list: The parsed view.
    """
    try:
        return set(os.environ['VIEW'].split(','))
    except (AttributeError, KeyError) as error:
        print('WARNING: Error parsing "VIEW" env var!')
        return set()
