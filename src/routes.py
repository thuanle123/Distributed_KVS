"""
Helped functions to create routes to endpoints
"""


def route(r=''):
    """
    Creates route for key value store

    Keyword Arguments:
        r {str} -- [The specefic node] (default: {''})

    Returns:
        [str] -- [The created route]
    """
    return '/key-value-store' + r


def route_shard(r=''):
    """
    Creates route for shard endpoint requests

    Keyword Arguments:
        r {str} -- [The specefic shard] (default: {''})

    Returns:
        [str] -- [The created route]
    """
    return '/key-value-store-shard' + r
