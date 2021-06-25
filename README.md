# flink cdc

cdc的学习
目前只是实现了cdc获取binlog的数据源

# 运行结果

创建mysql表，以及对应在sql-client的表

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.25/cdc_0.png)

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.25cdc_2.png)



执行binlogproducer， 可获取指定数据库更改的信息。在mysql交互式环境执行插入、更新，控制台打印出相应的binlog信息。

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.25cdc_1.png)



# 待完成

