# flink cdc

cdc的学习
目前只是实现了cdc获取binlog的数据源

# 运行结果

创建mysql表，以及对应在sql-client的表

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.25/cdc_0.png)

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.25cdc_2.png)



执行binlogproducer， 可获取指定数据库更改的信息。在mysql交互式环境执行插入、更新，控制台打印出相应的binlog信息。

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.25cdc_1.png)





### 下列操作在上述过程中改变了mysql表结构，加入了时间戳，主键，分区字段

由于对流处理的api还不熟悉，选择使用了更易用的SQL table api

执行生产者，并向mysql插入数据。

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.30/%E7%94%9F%E4%BA%A7.png)



提交消费者作业，使用存储过程插入80w条数据。



![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.30/consumer.png)



然后hdfs上显示hudi目录

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.30/hud.png)

![](http://quxjj6jyh.hn-bkt.clouddn.com/2021.6.30/hudi_log.png)





