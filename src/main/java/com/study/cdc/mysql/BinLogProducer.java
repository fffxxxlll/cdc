package com.study.cdc.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author feng xl
 * @date 2021/6/24 0024 21:50
 */

/**
 * 利用cdc连接器读取binlog，转成debezium-json格式，
 * 再写入到kafka
 *
 * */
public class BinLogProducer {

    public static void main(String[] args) throws Exception {
        /*初次使用cdc*/
//        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("hadoop01")
//                .port(3306)
//                .databaseList("flink_cdc")
//                .username("root")
//                .password("123456")
//                .deserializer(new StringDebeziumDeserializationSchema())
//                .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.addSource(sourceFunction);
//        env.execute();

        /* 流式table处理环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        /*创建读取binlog的表*/
        String sourceDDL = "CREATE TABLE mysql_binlog(" +
                        "id INT NOT NULL," +
                        "name STRING," +
                        "description STRING," +
                        "weight DECIMAL(10, 3)," +
                        "ts TIMESTAMP(3)," +
                        "`part` VARCHAR(20)" +
                        ") WITH (" +
                        " 'connector' = 'mysql-cdc'," +
                        " 'hostname' = 'hadoop01'," +
                        " 'port' = '3306'," +
                        " 'username' = 'root'," +
                        " 'password' = '123456'," +
                        " 'database-name' = 'flink_cdc'," +
                        " 'table-name' = 'test_cdc'" +
                        ")";

        /*写入kafka的表*/
        String sinkDDL = "CREATE TABLE binlog_sink(" +
                "id INT NOT NULL," +
                "name STRING," +
                "description STRING," +
                "weight DECIMAL(10, 3)," +
                "ts TIMESTAMP(3)," +
                "`part` VARCHAR(20)" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'binlog'," +
                " 'properties.bootstrap.servers' = 'hadoop01:9092'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'format' = 'debezium-json'" +
                ")";

        /*输入到控制台的表*/
        String sinkDDL2 = "CREATE TABLE console_sink(" +
                "id INT NOT NULL," +
                "name STRING," +
                "description STRING," +
                "weight DECIMAL(10, 3)," +
                "ts TIMESTAMP(3)," +
                "`part` VARCHAR(20)" +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        /*将binlog数据源写到kafka表*/
        String exeSql = "INSERT INTO binlog_sink SELECT * FROM mysql_binlog";

        /*将binlog数据源写到控制台表*/
        String exeSql2 = "INSERT INTO console_sink SELECT * FROM mysql_binlog";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(sinkDDL2);

        tableEnv.executeSql(exeSql);
        tableEnv.executeSql(exeSql2);


    }
}
