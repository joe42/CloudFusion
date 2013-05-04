import re

def to_unicode(text, format):
    if isinstance(text, unicode):
        return text
    return unicode(text, format)

def regSearchInt(needle, haystack, grp=1):
    match = re.search(needle,haystack)
    if not match:
        return None;
    if not match.group(grp):
        return None;
    return int(match.group(grp))
def regSearchString(needle, haystack, grp=1):
    match = re.search(needle,haystack)
    if not match:
        return None;
    if not match.group(grp):
        return None;
    return match.group(grp)
def indentLines(str, indent):
    """Indents a string's lines by *indent* spaces.
    Does not indent empty strings.
    """
    if str == "" or indent == 0:
        return str;
    ret = " "*indent;
    ret += str.replace("\n","\n"+" "*indent);
    ret = ret[:-indent]#remove trailing spaces
    return ret;

class RegExData():
    def __init__(self, regex, matchnr=1, type='int'):
        self.regex = regex;
        self.matchnr = matchnr;
        self.type = type; 