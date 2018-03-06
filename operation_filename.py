#!/usr/bin/env python
# -*- coding:utf-8 -*
import os
import logging
import codecs

"""
This module is used to format filename.
and operation file content encoding.
"""


def quote_filename(filename):
    basename = os.path.basename(filename)
    dirname = os.path.dirname(filename)
    return os.path.join(dirname, basename.replace(' ', '_'))


def conversion2csv(filename):
    filename = quote_filename(filename)
    dirname = os.path.dirname(filename)
    name, ext = os.path.splitext(filename)

    # if you want change ext to '.csv', you just need change this list
    if ext in ['.txt']:
        basename = name + '.csv'
        os.rename(filename, os.path.join(dirname, basename))
        filename = os.path.join(dirname, basename)
    # TODO maybe you need add other logic in there
    return filename


def convert_file_to_utf8(f, rv_file=None):
    if not rv_file:
        name, ext = os.path.splitext(f)
        if isinstance(name, unicode):
            name = name.encode('utf8')
        rv_file = '{}_utf8{}'.format(name, ext)
    logging.info('start to process file %s', f)
    with open(f) as infd:
        with open(rv_file, 'w') as outfd:
            lines = []
            loop = 0
            chunck = 200000
            first_line = infd.readline().strip(codecs.BOM_UTF8).strip() + '\n'
            lines.append(first_line)
            for line in infd:
                clean_line = line.decode('gb18030').encode('utf8')
                clean_line = clean_line.rstrip() + '\n'
                lines.append(clean_line)
                if len(lines) == chunck:
                    outfd.writelines(lines)
                    lines = []
                    loop += 1
                    logging.info('processed %s lines.', loop * chunck)

            outfd.writelines(lines)
            logging.info('processed %s lines.', loop * chunck + len(lines))
