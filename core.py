#!/usr/bin/env python
# -*- coding:utf-8 -*

import csv
import logging
import multiprocessing
import os
import warnings

import click
import MySQLdb
import sqlalchemy

from operation_filename import conversion2csv, convert_file_to_utf8
from operation_data import coversion2qq

warnings.filterwarnings('ignore', category=MySQLdb.Warning)

# Batch insert record number.
BATCH = 5000

DB_URI = 'mysql://root:root@127.0.0.1:3306/email_data?charset=utf8'

engine = sqlalchemy.create_engine(DB_URI)


def get_table_cols(table):
    sql = 'SELECT * FROM `{table}` LIMIT 0'.format(table=table)
    res = engine.execute(sql)
    return res.keys()


def insert_many(table, cols, rows, cursor):
    sql = 'INSERT INTO `{table}` ({cols}) VALUES ({marks})'.format(
        table=table,
        cols=', '.join(cols),
        marks=', '.join(['%s'] * len(cols)))
    try:
        cursor.execute(sql, *rows)
    # TODO improve this exception, to except specified error
    # except Exception is not good.
    # If you insert duplicate data, an error occurs(_mysql_exceptions.IntegrityError),
    # but catching this error can not be caught, so using Exception will improve later
    except Exception:
        pass
    logging.info('process %s inserted %s rows into table %s', os.getpid(), len(rows), table)


def insert_worker(table, cols, queue):
    rows = []
    # Each child process creates its own engine object
    cursor = sqlalchemy.create_engine(DB_URI)
    while True:
        row = queue.get()
        if row is None:
            if rows:
                insert_many(table, cols, rows, cursor)
            break

        row = coversion2qq(row)
        rows.append(row)
        if len(rows) == BATCH:
            insert_many(table, cols, rows, cursor)
            rows = []


def insert_parallel(table, reader, w=10):
    cols = get_table_cols(table)

    # Data queue, the main process of reading documents and write data inside, the worker process read data from the queue
    # Note the size of the control queue to avoid spending too slow lead to accumulation of too much data, take up too much memory
    queue = multiprocessing.Queue(maxsize=w * BATCH * 2)
    workers = []
    for i in range(w):
        p = multiprocessing.Process(target=insert_worker, args=(table, cols, queue))
        p.start()
        workers.append(p)
        logging.info('starting # %s worker process, pid: %s...', i + 1, p.pid)

    dirty_data_file = './{}_dirty_rows.csv'.format(table)
    xf = open(dirty_data_file, 'w')
    writer = csv.writer(xf, delimiter=reader.dialect.delimiter)

    for line in reader:
        # Record and skip dirty data: The number of keys is not consistent
        if len(line) != len(cols):
            writer.writerow(line)
            continue

        # Replace None with 'NULL'
        clean_line = [None if x == 'NULL' else x for x in line]

        # Write data to the queue
        queue.put(tuple(clean_line))
        if reader.line_num % 500000 == 0:
            logging.info('put %s tasks into queue.', reader.line_num)

    xf.close()

    # Send each worker a signal of the end of the task
    logging.info('send close signal to worker processes')
    for i in range(w):
        queue.put(None)

    for p in workers:
        p.join()


@click.group()
def cli():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


@cli.command('gbk_to_utf8')
@click.argument('f')
def convert_gbk_to_utf8(f):
    convert_file_to_utf8(f)


@cli.command('load')
@click.option('-t', '--table', required=True, help='表名')
@click.option('-i', '--filename', required=True, help='输入文件')
@click.option('-w', '--workers', default=10, help='worker 数量，默认 10')
def load_fac_day_pro_nos_sal_table(table, filename, workers):
    def operation_fd(fd):
        fd.readline()  # skip header
        reader = csv.reader(fd)
        insert_parallel(table, reader, w=workers)

    if os.path.isfile(filename):
        with open(conversion2csv(filename)) as fd:
            operation_fd(fd)
    elif os.path.isdir(filename):
        for dirpath, dirnames, filenames in os.walk(filename):
            if filenames:
                for fn in filenames:
                    with open(conversion2csv(os.path.join(dirpath, fn))) as fd:
                        operation_fd(fd)


if __name__ == '__main__':
    cli()
