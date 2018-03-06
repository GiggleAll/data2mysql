# data2mysql
This script is designed to efficiently import data into MySQL.
 
 ## keynote
- 批量插入而不是逐条插入
- 为了加快插入速度，先不要建索引
- 生产者和消费者模型，主进程读文件，多个 worker 进程执行插入
- 注意控制 worker 的数量，避免对 MySQL 造成太大的压力
- 注意处理脏数据导致的异常
- 原始数据是 GBK 编码，所以还要注意转换成 UTF-8
- 用 click 封装命令行工具

