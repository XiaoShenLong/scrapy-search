#新建数据库
mysql -uroot -p
create database baike
exit
#建表
python baike/mysql/mysqlconn.py
#新建es索引
python baike/models/es_types.py
#开始爬虫
scrapy crawl baike
#redis-cli
lpush baike:start_urls https://baike.baidu.com/



