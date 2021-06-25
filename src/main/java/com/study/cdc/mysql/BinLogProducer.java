package com.study.cdc.mysql;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author feng xl
 * @date 2021/6/24 0024 21:50
 */
public class BinLogProducer {
    public static void main(String[] args) throws Exception {
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop01")
                .port(3306)
                .databaseList("flink_cdc")
                .username("root")
                .password("123456")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction).print().setParallelism(1);
        env.execute();
    }
}
