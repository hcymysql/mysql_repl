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
import json


# 设置日志记录
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

# 控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)

# info日志文件处理器
info_file_handler = logging.FileHandler('mysql_repl_info.log')
info_file_handler.setLevel(logging.INFO)
info_file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
info_file_handler.setFormatter(info_file_format)
logger.addHandler(info_file_handler)

# error日志文件处理器
error_file_handler = logging.FileHandler('mysql_repl_error.log')
error_file_handler.setLevel(logging.ERROR)
error_file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_file_handler.setFormatter(error_file_format)
logger.addHandler(error_file_handler)


#########################################################################################################################
def parse_args():
    parser = argparse.ArgumentParser(description='MySQL binlog replication tool')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the YAML config file')
    parser.add_argument('-v', '--version', action='version', version='mysql_repl工具版本号: 1.0.3，更新日期：2024-08-15')
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

def save_binlog_pos(binlog_file, binlog_pos):
    current_binlog_file = binlog_file
    with open('binlog_info.txt', 'w') as f:
        f.write(f'{current_binlog_file}\n{binlog_pos}')
    print(f'Binlog position ({current_binlog_file}, {binlog_pos}) saved.')

def load_binlog_pos():
    global binlog_file, binlog_pos
    try:
        with open('binlog_info.txt', 'r') as f:
            binlog_file, binlog_pos = f.read().strip().split('\n')
    except FileNotFoundError:
        pass
    except Exception as e:
        print('Load binlog position failure:', e)
    
    return binlog_file, int(binlog_pos)

def exit_handler(stream, current_binlog_file, binlog_pos):
    stream.close()
    save_binlog_pos(current_binlog_file, binlog_pos or stream.log_pos)

def save_binlog_pos_on_termination(signum, frame):
    save_binlog_pos(current_binlog_file, binlog_pos or stream.log_pos)
    quit_program()

def quit_program():
    stream.close()
    target_conn.close()
    exit(0)

target_conn = pymysql.connect(**target_mysql_settings)

saved_pos = load_binlog_pos()

sql_queue = Queue()

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


def convert_bytes_keys(obj):
    if isinstance(obj, dict):
        return {k.decode('utf-8') if isinstance(k, bytes) else k: convert_bytes_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bytes_keys(i) for i in obj]
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    else:
        return obj


def escape_string(v):
    return str(v).replace("'", "''")


def process_rows_event(binlogevent, stream):
    if hasattr(binlogevent, "schema"):
        database_name = binlogevent.schema
    else:
        database_name = None

    global event_time
    event_time = binlogevent.timestamp

    if isinstance(binlogevent, QueryEvent):
        sql = binlogevent.query
        logger.info(sql)
        sql_queue.put(sql)
    else:
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                columns = []
                values = []
                for k, v in row["values"].items():
                    columns.append(f"`{k}`")
                    if isinstance(v, (str, datetime.datetime, datetime.date)):
                        values.append(f"'{escape_string(v)}'")
                    elif v is None:
                        values.append('NULL')
                    elif isinstance(v, dict):
                        v = convert_bytes_keys(v)
                        values.append(f"'{escape_string(json.dumps(v, ensure_ascii=False))}'")
                    elif isinstance(v, bytes):
                        values.append(f"'{escape_string(v.decode('utf-8'))}'")
                    else:
                        values.append(str(v))

                sql = f"INSERT INTO `{database_name}`.`{binlogevent.table}` ({','.join(columns)}) VALUES ({','.join(values)});"
                logger.info(sql)
                sql_queue.put(sql)

            elif isinstance(binlogevent, UpdateRowsEvent):
                set_values = []
                where_values = []
                for k, v in row["after_values"].items():
                    if isinstance(v, (str, datetime.datetime, datetime.date)):
                        set_values.append(f"`{k}`='{escape_string(v)}'")
                    elif v is None:
                        set_values.append(f"`{k}`=NULL")
                    elif isinstance(v, dict):
                        v = convert_bytes_keys(v)
                        set_values.append(f"`{k}`='{escape_string(json.dumps(v, ensure_ascii=False))}'")
                    elif isinstance(v, bytes):
                        set_values.append(f"`{k}`='{escape_string(v.decode('utf-8'))}'")
                    else:
                        set_values.append(f"`{k}`={v}")

                for k, v in row["before_values"].items():
                    if isinstance(v, (str, datetime.datetime, datetime.date)):
                        where_values.append(f"`{k}`='{escape_string(v)}'")
                    elif v is None:
                        where_values.append(f"`{k}` IS NULL")
                    elif isinstance(v, dict):
                        v = convert_bytes_keys(v)
                        json_str = json.dumps(v, ensure_ascii=False)
                        where_values.append(f"JSON_CONTAINS(`{k}`, '{escape_string(json_str)}')")
                    elif isinstance(v, bytes):
                        where_values.append(f"`{k}`='{escape_string(v.decode('utf-8'))}'")
                    else:
                        where_values.append(f"`{k}`={v}")

                set_clause = ','.join(set_values)
                where_clause = ' AND '.join(where_values)

                sql = f"UPDATE `{database_name}`.`{binlogevent.table}` SET {set_clause} WHERE {where_clause};"
                logger.info(sql)
                sql_queue.put(sql)

            elif isinstance(binlogevent, DeleteRowsEvent):
                where_values = []
                for k, v in row["values"].items():
                    if isinstance(v, (str, datetime.datetime, datetime.date)):
                        where_values.append(f"`{k}`='{escape_string(v)}'")
                    elif v is None:
                        where_values.append(f"`{k}` IS NULL")
                    elif isinstance(v, dict):
                        v = convert_bytes_keys(v)
                        json_str = json.dumps(v, ensure_ascii=False)
                        where_values.append(f"JSON_CONTAINS(`{k}`, '{escape_string(json_str)}')")
                    elif isinstance(v, bytes):
                        where_values.append(f"`{k}`='{escape_string(v.decode('utf-8'))}'")
                    else:
                        where_values.append(f"`{k}`={v}")

                where_clause = ' AND '.join(where_values)
                sql = f"DELETE FROM `{database_name}`.`{binlogevent.table}` WHERE {where_clause};"
                logger.info(sql)
                sql_queue.put(sql)

    return binlogevent.packet.log_pos


while True:
    try:
        for binlogevent in stream:
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

sql_queue.join()

atexit.register(exit_handler, stream, current_binlog_file, binlog_pos)

signal.signal(signal.SIGTERM, save_binlog_pos_on_termination)
signal.signal(signal.SIGINT, save_binlog_pos_on_termination)

atexit.register(target_conn.close)

