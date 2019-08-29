from datetime import datetime
import requests
import re


def get_string(iterator, literal_terminator='"'):
    """Gets a string enclosed in a literal terminator.

    The first string in the iterator is already part of the returned string.

    Args:
        iterator: A string iterator.
        literal_terminator: defines the string ending character (defaults to ").

    Returns:
        The enclosed string.
    """
    in_string_literal = True
    str_ = ''
    while in_string_literal:
        token = iterator.next()
        if token in literal_terminator:
            in_string_literal = False
        else:
            str_ += token
    return str_


def date_str_to_datetime(date_str, delimiter='-'):
    """Converts date string to datetime object

    Args:
        date_str: A string represents a date, delimited by the delimiter parameter
            e.g. 1970-01-01 (delimiter is '-')
        delimiter: Delimites the different parts of the date string (defaults to '-')

    Returns:
        The datetime object.
    """
    return datetime(*map(int, date_str.split(delimiter)))


def timestamp_to_datetime(timestamp_str, date_delim='-', time_delim=':'):
    """Converts datetime string to datetime object

    Args:
        date_str: A string represents a datetime, delimited by the delimiter parameters
            e.g. 1970-01-01 00:00:00 (delimiters are '-' and ':')
        date_delim: Delimites the different parts of the date string (defaults to '-')
        time_delim: Delimites the different parts of the time string (defaults to ':')

    Returns:
        The datetime object.
    """
    return datetime(*map(int, re.split('-|:| ', timestamp_str)))


def datetime_to_epoch(dt_object):
    """Converts datetime object to seconds since epoch.

    Args:
        dt_object: A datetime object.

    Returns:
        Seconds since epoch (integer).
    """
    return int((dt_object - datetime(1970, 1, 1)).total_seconds())


def api_request(address):
    """Makes a get request.

    Args:
        address: the url address for the api get request.

    Returns:
        The reponse object.

    Raises:
        HTTPError
    """
    r = requests.get(address)
    if r.status_code != 200:
        error_message = "bad status code: {0}".format(r.status_code)
        raise requests.exceptions.HTTPError(error_message)
    return r
