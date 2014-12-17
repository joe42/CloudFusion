import re
import base64
import ntplib
import time

def __num_to_alpha(num):
    '''From: http://stackoverflow.com/questions/10326118/encoding-a-numeric-string-into-a-shortened-alphanumeric-string-and-back-again'''
    num = hex(num)[2:].rstrip("L")
    if len(num) % 2:
        num = "0" + num
    ret = base64.b32encode(num.decode('hex'))
    return ret

def get_uuid():
    '''Get globally unique identifier'''
    try:
        time_offset = ntplib.NTPClient().request('pool.ntp.org').offset
    except Exception, e:
        time_offset = 0
    unique_num = int( 100 * (time.time() + time_offset) ) #get globally unique number
    ret = __num_to_alpha(unique_num)
    return ret

def get_id_key(dictionary):
    '''Get the key for the id defined in dictionary from a list of synonyms.
    Cloud providers have different names for the id like amazon's access_key_id or Dropbox' consumer_key.
    :returns: key from *dicitionary* which is a synonym for an cloud account id or None otherwise'''
    ID_SYNONYMS = ['access_key_id', 'aws_access_key_id', 'gs_access_key_id', 'access_key_id', 'consumer_key', 'client_id', 'id']
    try:
        return (s for s in ID_SYNONYMS if s in dictionary).next()
    except StopIteration:
        return None
    
def get_secret_key(dictionary):
    '''Get the key for the secret defined in dictionary from a list of synonyms.
    Cloud providers have different names for the secret like amazon's secret_access_key or Dropbox' consumer_secret.
    :returns: key from *dicitionary* which is a synonym for an cloud account secret or None otherwise'''
    SECRET_SYNONYMS = ['private_access_key', 'aws_secret_access_key', 'gs_secret_access_key', 'secret_access_key', 'consumer_secret', 'client_secret', 'secret']
    try:
        return (s for s in SECRET_SYNONYMS if s in dictionary).next()
    except StopIteration:
        return None

def to_unicode(text, encoding="utf8"):
    ''':returns: *text* as unicode, decoded with *encoding*, if it is a bytestring,
    and returns *text* otherwise'''
    if isinstance(text, str):
        return unicode(text, encoding)
    return text

def to_str(text, encoding="utf8"):
    ''':returns: *text* as a byte string, encoded with *encoding*, 
    if it is a unicode object and returns *text* otherwise'''
    if isinstance(text, unicode):
        return text.encode(encoding)
    return text

def regSearchInt(needle, haystack, grp=1):
    match = re.search(needle,haystack)
    if not match:
        return None
    if not match.group(grp):
        return None
    return int(match.group(grp))
def regSearchString(needle, haystack, grp=1):
    match = re.search(needle,haystack)
    if not match:
        return None
    if not match.group(grp):
        return None
    return match.group(grp)
def indentLines(string, indent):
    """Indents a string's lines by *indent* spaces.
    Does not indent empty strings.
    """
    if string == "" or indent == 0:
        return string
    ret = " "*indent
    ret += string.replace("\n","\n"+" "*indent)
    ret = ret[:-indent]#remove trailing spaces
    return ret

class RegExData():
    def __init__(self, regex, matchnr=1, type='int'):
        self.regex = regex
        self.matchnr = matchnr
        self.type = type
