# mysql_repl - 轻便型的MySQL binlog replication tool
# https://github.com/hcymysql/mysql_repl

## 使用场景：
- 从MySQL8.0实时解析binlog并复制到MariaDB，适用于将MySQL8.0迁移至MariaDB
- 不熟悉MySQL主从复制搭建的新手使用

## 原理：
1) 把自己伪装成slave，从源master解析binlog并入库target MySQL端。
2) 必须按照如下要求设置MySQL这两个参数值
```
   binlog_format=ROW
   binlog_row_image=FULL
```

## 使用：
```
shell> chmod 755 mysql_repl
shell> ./mysql_repl -c test.yaml
```
![784767b9318df117322a6c8d51026a9](https://github.com/hcymysql/mysql_repl/assets/19261879/8e7c52b9-50c5-4108-814a-1389ae496f31)

## 注意事项：
- mysql_repl工具会一直运行在终端，如需停止按住<ctrl+c>终止。
- 支持断点续传，会实时记录解析后的binlog信息至当前目录下的binlog_info.txt文件里。
- 实时同步复制信息记录至mysql_repl_info.log文件里。
- 实时同步复制报错信息记录至mysql_repl_error.log文件里。
