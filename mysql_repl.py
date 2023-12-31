#!/usr/bin/env python3

import os
import pymysql
import signal
import atexit
import time, datetime
from queue import Queue
from threading import Thread
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.event import GtidEvent
import yaml
import argparse
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False  # 防止日志消息传播到根记录器

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)

def parse_args():
    parser = argparse.ArgumentParser(description='MySQL binlog replication tool')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the YAML config file')
    parser.add_argument('-v', '--version', action='version', version='mysql_repl工具版本号: 1.0.1，更新日期：2023-12-14')
    return parser.parse_args()

args = parse_args()
config_file = args.config

# 读取YAML配置文件
with open(config_file, 'r') as stream:
    config = yaml.safe_load(stream)

source_mysql_settings = config['source_mysql_settings']
source_server_id = config['source_server_id']
binlog_file = config['binlog_file']
binlog_pos = config['binlog_pos']
target_mysql_settings = config['target_mysql_settings']

# 保存 binlog 位置和文件名
def save_binlog_pos(binlog_file, binlog_pos):
    current_binlog_file = binlog_file
    if binlog_file is not None:
        with open('binlog_info.txt', 'w') as f:
            f.write('{}\n{}'.format(current_binlog_file, binlog_pos))
        print('Binlog position ({}, {}) saved.'.format(current_binlog_file, binlog_pos))
    else:
        with open('binlog_info.txt', 'w') as f:
            f.write('{}\n{}'.format(current_binlog_file, binlog_pos))
        print('Binlog position ({}, {}) updated.'.format(current_binlog_file, binlog_pos))


# 读取上次保存的 binlog 位置和文件名
def load_binlog_pos():
    global binlog_file, binlog_pos
    try:
        with open('binlog_info.txt', 'r') as f:
            binlog_file, binlog_pos = f.read().strip().split('\n')
    except FileNotFoundError:
        binlog_file, binlog_pos = binlog_file, binlog_pos
    except Exception as e:
        print('Load binlog position failure:', e)
        binlog_file, binlog_pos = binlog_file, binlog_pos

    return binlog_file, int(binlog_pos)


# 退出程序时保存当前的 binlog 文件名和位置点
def exit_handler(stream, current_binlog_file, binlog_pos):
    stream.close()
    # save_binlog_pos(current_binlog_file, binlog_pos)
    save_binlog_pos(current_binlog_file, binlog_pos or stream.log_pos)


# 在程序被终止时保存当前的 binlog 文件名和位置点
def save_binlog_pos_on_termination(signum, frame):
    save_binlog_pos(current_binlog_file, binlog_pos or stream.log_pos)
    quit_program()


# 退出程序时保存当前的 binlog 文件名和位置点
def quit_program():
    stream.close()
    target_conn.close()
    exit(0)


# 建立连接
target_conn = pymysql.connect(**target_mysql_settings)

saved_pos = load_binlog_pos()

# 定义队列用于存放 SQL 语句并作为解析 binlog 和执行 SQL 语句的中间件
sql_queue = Queue()


# 定义 SQL 执行函数，从队列中取出 SQL 语句并依次执行
def sql_worker():
    while True:
        sql = sql_queue.get()
        try:
            with target_conn.cursor() as cursor:
                cursor.execute(sql)
                target_conn.commit()
                logger.info(f"target mysql - success to execute SQL: {sql}")
                current_timestamp = int(time.time())
                Seconds_Behind_Master = current_timestamp - event_time
                logger.info(f"入库延迟时间为：{Seconds_Behind_Master} （单位秒）")
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}")
            logger.error(f"Error message: {e}")
        finally:
            sql_queue.task_done()


# 启动 SQL 执行线程
sql_thread = Thread(target=sql_worker, daemon=True)
sql_thread.start()

stream = BinLogStreamReader(
    connection_settings=source_mysql_settings,
    server_id=source_server_id,
    blocking=True,
    resume_stream=True,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
    log_file=saved_pos[0],
    log_pos=saved_pos[1]
)


# 循环遍历解析出来的行事件并存入SQL语句中
def process_rows_event(binlogevent, stream):
    if hasattr(binlogevent, "schema"):
        database_name = binlogevent.schema  # 获取数据库名
    else:
        database_name = None

    global event_time
    event_time = binlogevent.timestamp

    if isinstance(binlogevent, QueryEvent):
        sql = binlogevent.query
        logger.info(sql)
        sql_queue.put(sql)  # 将 SQL 语句加入队列
    else:
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                #print("Table:", binlogevent.table)
                #print("Database:", database_name)
                #print("Column Names:", [k for k in row["values"].keys()])
                #print("Values:", [v for v in row["values"].values()])
                sql = "INSERT INTO {}({}) VALUES ({});".format(
                    f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                    ','.join(["`{}`".format(k) for k in row["values"].keys()]),
                    ','.join(["'{}'".format(v) if isinstance(v, (
                        str, datetime.datetime, datetime.date)) else 'NULL' if v is None else str(v)
                              for v in row["values"].values()])
                )
                logger.info(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列

            elif isinstance(binlogevent, UpdateRowsEvent):
                set_values = []
                for k, v in row["after_values"].items():
                    if isinstance(v, str):
                        set_values.append(f"`{k}`='{v}'")
                    elif isinstance(v, (datetime.datetime, datetime.date)):
                        set_values.append(f"`{k}`='{v}'")  # 将时间字段转换为字符串形式
                    else:
                        set_values.append(f"`{k}`={v}" if v is not None else f"`{k}`= NULL")
                set_clause = ','.join(set_values)

                where_values = []
                for k, v in row["before_values"].items():
                    if isinstance(v, str):
                        where_values.append(f"`{k}`='{v}'")
                    elif isinstance(v, (datetime.datetime, datetime.date)):
                        where_values.append(f"`{k}`='{v}'")  # 添加对时间类型的处理
                    else:
                        where_values.append(f"`{k}`={v}" if v is not None else f"`{k}` IS NULL")
                where_clause = ' AND '.join(where_values)

                sql = f"UPDATE `{database_name}`.`{binlogevent.table}` SET {set_clause} WHERE {where_clause};"
                logger.info(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列

            elif isinstance(binlogevent, DeleteRowsEvent):
                sql = "DELETE FROM {} WHERE {};".format(
                    f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                    ' AND '.join(
                        ["`{}`={}".format(k, "'{}'".format(v) if isinstance(v, (str, datetime.datetime, datetime.date))
                        else 'NULL' if v is None else str(v))
                         for k, v in row["values"].items()])
                )
                logger.info(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列

    return binlogevent.packet.log_pos


# 循环遍历解析出来的行事件并存入SQL语句中
while True:
    try:
        for binlogevent in stream:
            # print(f'binlog: {stream.log_file}, positon: {stream.log_pos}')
            current_binlog_file = stream.log_file
            try:
                binlog_pos = process_rows_event(binlogevent, stream)
            except AttributeError as e:
                save_binlog_pos(current_binlog_file, binlog_pos)
            else:
                save_binlog_pos(current_binlog_file, binlog_pos)

    except KeyboardInterrupt:
        save_binlog_pos(current_binlog_file, binlog_pos)
        break

    except pymysql.err.OperationalError as e:
        logger.error("MySQL Error {}: {}".format(e.args[0], e.args[1]))

# 等待所有 SQL 语句执行完毕
sql_queue.join()

# 在程序退出时保存 binlog 位置
atexit.register(exit_handler, stream, current_binlog_file, binlog_pos)

# 接收 SIGTERM 和 SIGINT 信号
signal.signal(signal.SIGTERM, save_binlog_pos_on_termination)
signal.signal(signal.SIGINT, save_binlog_pos_on_termination)

# 关闭连接
atexit.register(target_conn.close)
