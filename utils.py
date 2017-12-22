from math import sqrt
from itertools import count
from collections import OrderedDict
import re
import hashlib
import random
import logging
import subprocess
import resource
import socket
import time

# python3 -- type compatibility
from six import string_types

log = logging.getLogger('cloud_provision')

def mean( xs ):
    """
    :param xs: a list containing the sequence of values to be evaluated
    :rtype: the mean value of a sequence of values.
    """

    try:
        return sum( xs ) / float( len( xs ) )
    except TypeError:
        raise ValueError( "Input can't have non-numeric elements" )
    except ZeroDivisionError:
        raise ValueError( "Input can't be empty" )


def std_dev( xs ):
    """
    :param xs: a list containing the sequence of values to be evaluated
    :rtype: the standard deviation of the given iterable of numbers.
    """
    m = mean( xs )  # this checks our pre-conditions, too
    return sqrt( sum( (x - m) ** 2 for x in xs ) / float( len( xs ) ) )


def camel_to_snake( s, separator='_' ):
    """
    Converts camel to snake case
    (i.e., lower-case with underscore between words)

    >>> camel_to_snake('CamelCase')
    'camel_case'

    >>> camel_to_snake('AWS310Box',separator='-')
    'aws-310-box'
    """
    s = re.sub( '([a-z0-9])([A-Z])', r'\1%s\2' % separator, s )
    s = re.sub( '([a-z])([A-Z0-9])', r'\1%s\2' % separator, s )
    return s.lower( )


def snake_to_camel( s, separator='_' ):
    """
    Converts snake to camel case
    (i.e., each word or abbreviation in the middle of the phrase
    begins with a capital letter, with no intervening spaces or punctuation)

    >>> snake_to_camel('_x____yz')
    'XYz'

    >>> snake_to_camel('camel_case')
    'CamelCase'
    """
    return ''.join( [ w.capitalize( ) for w in s.split( separator ) ] )


def randomizeID( a_str, num_digits=8  ):
    """
    Generate a unique identifier of length 8.
    Hashes the given string (or '__name__' if string not provided)
    and returns 8 random characters from the hashed string.

    Safe enough from collisions. Stronger if a string is provided.
    """

    if a_str is None:
        a_str = '__name__'

    namehash = hashlib.sha256(a_str.encode("UTF-8")).hexdigest()
    return ''.join(random.choice(namehash) for x in range(num_digits))

def to_aws_name( name ):
        """
        Returns a name that is safe to use for resource
        names on AWS.
        """
        return name.lower().replace( '_', '__' ).replace( '/', '_' )


def is_absolute_name( name ):
    """
    Returns true if a name refers to an absolute name or absolute path
    (i.e., /path/to/some/resource is an absolute name)
    """
    return name[0:1] == '/'


def base_name( name ):
    """
    Returns the base name of an absolute name
    (i.e., in '/path/to/some/resource' the base name is 'resource')
    """
    return name.split('/')[-1]


def drop_hostname( email ):
    """
    Returns the 'user' name in a hostname written in the format 'user@domain'
    """
    try:
        n = email.index( "@" )
    except ValueError:
        return email
    else:
        return email[0:n]


def system(command):
    """
    A wrapper around subprocess.check_call.
    Logs the command if in debug mode.
    If the command is a string, shell=True will be passed to subprocess.check_call.

    :type command: str | sequence[string]
    """
    log.debug('Running: %r', command)
    subprocess.check_call(command, shell=isinstance(command, string_types), bufsize=-1)


def totalCpuTimeAndMemoryUsage():
    """
    Gives the total cpu time of itself and all its children, and the maximum RSS memory usage of
    itself and its single largest child.

    :return: tuple containing CPUtime and MEMusage
    """
    me = resource.getrusage(resource.RUSAGE_SELF) # info about the process itself
    childs = resource.getrusage(resource.RUSAGE_CHILDREN) # info about the calling process

    totalCPUTime = me.ru_utime + me.ru_stime + childs.ru_utime + childs.ru_stime
    totalMemoryUsage = me.ru_maxrss + childs.ru_maxrss

    return totalCPUTime, totalMemoryUsage


def getTotalCpuTime():
    """Gives the total cpu time, including the children.
    """
    return totalCpuTimeAndMemoryUsage()[0]


def getTotalMemoryUsage():
    """Gets the amount of memory used by the process and its largest child.
    """
    return totalCpuTimeAndMemoryUsage()[1]


def waitForOpenPort(ip_address, port=22):
    """
    Wait until the given IP address is accessible via the specified port.
    The default port is 22 (SSH)

    :return: the number of unsuccessful attempts to connect to the port before a the first
    success
    """
    log.info('Waiting for port %s on %s to be open...', port, ip_address)
    i = 0
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(5.0)
            ret = s.connect_ex((ip_address, port))
            if ret == 0:
                log.info('...port open')
                break
            else:
                time.sleep(3.1)
                i += 1
        except socket.error:
            continue
        finally:
            s.close()

    return i

def uniqueList(seq_list):
    #uniques = set(seq_list)
    #return list(uniques)
    return list(OrderedDict.fromkeys(seq_list))

def privateToPublicKey(priv_key_path):
    from Crypto.PublicKey import RSA
    priv_key = open(priv_key_path, 'r').read()
    kpriv = RSA.importKey(priv_key)
    public_key = kpriv.publickey().exportKey('OpenSSH')

    return public_key



class Map(dict):
    '''
    A Map object, i.e. a dictionary that can be accessed and modified using
    the dot notation.

    Example:
        >>> m = Map({'first_name': 'Eduardo'},
                    last_name='Pool',
                    age=24,
                    sports=['Soccer']
                )
        >>> m.last_name
        Pool
        >>>
    '''

    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.iteritems():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.iteritems():
                self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]

    def __missing__(self,key):
        value = self[key] = type(self)();
        return value

    # shallow copy
    def copy(self):
        return type(self)(self)


import collections
def flatten(iterables):
    for el in iterables:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el


class combine(object):
    def __init__( self, *args ):
        super( combine, self ).__init__( )
        self.args = args

    def __iter__( self ):
        def expand( x ):
            if isinstance( x, combine ) and len( x.args ) == 1:
                i = x.args
            else:
                try:
                    i = x.__iter__( )
                except AttributeError:
                    i = x,
            return i

        return flatten( map( expand, self.args ) )



class UserError( RuntimeError ):
    def __init__( self, message=None, cause=None ):
        if message is None and cause is None:
            raise RuntimeError( "Must pass either message or cause." )
        super( UserError, self ).__init__( message if cause is None else cause.message )
