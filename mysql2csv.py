#!/usr/bin/env python
# -*- coding:utf-8 -*
"""This module work for read mysql database into csv file"""
import csv
import MySQLdb


def main():
    # 连接数据库
    conn = MySQLdb.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='root',
        db='email_data',
    )
    cur = conn.cursor()

    # 以写的方式打开 csv 文件并将内容写入到w
    f = open("./output_CSV_file.csv", 'w')
    write_file = csv.writer(f)

    # 从 student 表里面读出数据，写入到 csv 文件里
    cur.execute("select * from example")
    while True:
        row = cur.fetchone()  # 获取下一个查询结果集为一个对象
        if not row:
            break
        write_file.writerow(row)  # csv模块方法一行一行写入
    f.close()

    # 关闭连接
    if cur:
        cur.close()
    if conn:
        conn.close()


if __name__ == '__main__':
    main()