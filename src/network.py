from collections import namedtuple
from enum import Enum
import concurrent.futures
import requests

UnicastResponse = namedtuple('UnicastResponse', ['uri', 'address', 'response'])
HTTPMethods = Enum('HTTPMethods', 'GET POST PUT DELETE')


def unicast(address, address_to_uri, http_method=HTTPMethods.GET, timeout=None, data=None, headers=None):
    uri = address_to_uri(address)

    try:
        if http_method == HTTPMethods.GET:
            resp = requests.get(uri, timeout=timeout, headers=headers)
        elif http_method == HTTPMethods.PUT:
            resp = requests.put(uri, timeout=timeout,
                                data=data, headers=headers)
        elif http_method == HTTPMethods.POST:
            resp = requests.post(uri, timeout=timeout,
                                 data=data, headers=headers)
        elif http_method == HTTPMethods.DELETE:
            resp = requests.delete(uri, timeout=timeout,
                                   data=data, headers=headers)
        else:
            resp = None
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
        resp = None

    return UnicastResponse(uri, address, resp)


def multicast(addresses, address_to_uri, http_method=HTTPMethods.GET, timeout=None, data=None, headers=None):
    fs = []
    max_workers = min(len(addresses), 10)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for address in addresses:
            unicast_response_future = executor.submit(
                unicast,
                address,
                address_to_uri,
                http_method=http_method,
                timeout=timeout,
                data=data,
                headers=headers
            )
            fs.append(unicast_response_future)
    return fs
