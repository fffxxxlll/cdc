package com.study.cdc.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author feng xl
 * @date 2021/6/24 0024 21:50
 */

/**
 * 读取kafka的binlog，然后写入hudi
 * */
public class BinLogConsumer {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        /*创建binlog数据表*/
        String sourceDDL = "CREATE TABLE binlog_source(" +
                "id INT PRIMARY KEY NOT ENFORCED," +
                "name STRING," +
                "description STRING," +
                "weight DECIMAL(10, 3)," +
                "ts TIMESTAMP(3)," +
                "`part` VARCHAR(20)" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'binlog'," +
                " 'properties.bootstrap.servers' = 'hadoop01:9092'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'format' = 'debezium-json'" +
                ")";

        /*流入hudi的表*/
        String sinkDDL = "CREATE TABLE sink_hudi(" +
                "id INT PRIMARY KEY NOT ENFORCED," +
                "name STRING," +
                "description STRING," +
                "weight DECIMAL(10, 3)," +
                "ts TIMESTAMP(3)," +
                "`part` VARCHAR(20)" +
                ") PARTITIONED BY (`part`) WITH (" +
                " 'connector' = 'hudi'," +
                " 'table.type' = 'MERGE_ON_READ'," +
                " 'path' = 'hdfs://hadoop01:8020/tmp/hudi/mysql_binlog'," +
                " 'read.streaming.enabled' = 'true'," +
                " 'read.streaming.check-interval' = '1'" +
                ")";

        /*将获取的binlog写入hudi*/
        String exeSql = "INSERT INTO sink_hudi SELECT * FROM binlog_source";
        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(exeSql);

    }
}
