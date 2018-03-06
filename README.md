# data2mysql
This script is designed to efficiently import data into MySQL.
 
 ## keynote
- Batch insert rather than insert.
- To speed up the insertion, do not index.
- Producer and consumer models, the main process of reading documents, the implementation of the process of inserting multiple workers
- Pay attention to controlling the number of workers, to avoid causing too much pressure on MySQL
- Attention to dealing with abnormal data caused by dirty
- The original data is GBK encoding, so pay attention to convert to UTF-8
- Use the click package command line tool

