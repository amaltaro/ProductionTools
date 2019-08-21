#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : test_blocks.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
"""

# system modules
import os
import re
import sys
import json
import argparse
from traceback import print_exc

try:
    import cStringIO as StringIO
except ImportError:
    import io as StringIO # python3
except ImportError:
    import StringIO

# psutil module
import psutil

import sys
from types import ModuleType, FunctionType
from gc import get_referents

def getSize(obj):
    """
    _getSize_
    Function to traverse an object and calculate its total size in bytes
    :param obj: a python object
    :return: an integer representing the total size of the object
    Code extracted from Stack Overflow:
    https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
    """
    # Custom objects know their class.
    # Function objects seem to know way too much, including modules.
    # Exclude modules as well.
    BLACKLIST = type, ModuleType, FunctionType

    if isinstance(obj, BLACKLIST):
        raise TypeError('getSize() does not take argument of type: '+ str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return size

float_number_pattern = \
    re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')
int_number_pattern = \
    re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input file")

def size_format(uinput):
    """
    Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    if  not (float_number_pattern.match(str(uinput)) or \
                int_number_pattern.match(str(uinput))):
        return 'N/A'
    try:
        num = float(uinput)
    except Exception as exc:
        print_exc(exc)
        return "N/A"
    base = 1000. # CMS convention to use power of 10
    if  base == 1000.: # power of 10
        xlist = ['', 'KB', 'MB', 'GB', 'TB', 'PB']
    elif base == 1024.: # power of 2
        xlist = ['', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    for xxx in xlist:
        if  num < base:
            return "%3.1f%s" % (num, xxx)
        num /= base

def load(fin):
    with open(fin) as jo:
        data = json.load(jo)
    return data

def create_stream(data):
    sdata = '[\n'
    for idx, rec in enumerate(data):
        if idx:
            sdata += '\n,\n'
        sdata += json.dumps(rec)
    sdata += '\n]'
    return StringIO.StringIO(sdata)

def parse_stream(stream):
    data = []
    while True:
        line = stream.readline().replace('\n', '').strip()
        if line == '[':
            continue
        if line.startswith('{') and line.endswith('}'):
            rec = json.loads(line)
            data.append(rec)
        if line == ']':
            break
    return data

def print_mem(obj, data, mem, memIni=None):
    print("\nMemory usage for object: %s" % obj)
    #tot = mem.total
    #use = mem.used
    if memIni:
        tot = getattr(mem, "total", 0) - getattr(memIni, "total", 0)
        rss = getattr(mem, "rss", 0) - getattr(memIni, "rss", 0)
        pss = getattr(mem, "pss", 0) - getattr(memIni, "pss", 0)
        uss = getattr(mem, "uss", 0) - getattr(memIni, "uss", 0)
    else:
        tot = getattr(mem, "total", 0)
        rss = getattr(mem, "rss", 0)
        pss = getattr(mem, "pss", 0)
        uss = getattr(mem, "uss", 0)
    print('object  memory: %s' % getSize(data))
    print('Total   memory: %s (%s)' % (tot, size_format(tot)))
    print('RSS     memory: %s (%s)' % (rss, size_format(rss)))
    print('PSS     memory: %s (%s)' % (pss, size_format(pss)))
    print('USS     memory: %s (%s)' % (uss, size_format(uss)))

def test(fin):
    "Perform main test with json data"
    data = None
    pid = psutil.Process()
    mem = pid.memory_full_info()
    print_mem('nothing', data, mem)

    data = load(fin)
    mem = pid.memory_full_info()
    print_mem('json', data, mem)

    memIni = pid.memory_full_info()
    stream = create_stream(data)
    mem = pid.memory_full_info()
    print_mem('stream', stream, mem, memIni)

    memIni = pid.memory_full_info()
    data1 = parse_stream(stream)
    mem = pid.memory_full_info()
    print_mem('json out of stream', data1, mem, memIni)

    if data != data1:
        print("data mismatch")
        print("data type: %s" % type(data))
        print("data1 type: %s" % type(data1))


def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    test(opts.fin)

if __name__ == '__main__':
    main()
