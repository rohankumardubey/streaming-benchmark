package org.viirya.flink.streaming.benchmark;

import java.util.Properties;
import java.util.UUID;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.Rowtime;

public class KafkaRead {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: KafkaRead --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        Properties kparams = params.getProperties();
        kparams.setProperty("auto.offset.reset", "latest");
        kparams.setProperty("flink.starting-position", "latest");
        kparams.setProperty("group.id", UUID.randomUUID().toString());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Source Table.
        String createSource = "CREATE TABLE sourceTopic (\n" +
                "  `artist` STRING,\n" +
                "  `auth` STRING,\n" +
                "  `city` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + params.getRequired("read-topic") + "',\n" +
                "  'properties.bootstrap.servers' = '" + params.getRequired("bootstrap.servers") + "',\n" +
                "  'properties.group.id' = '" + UUID.randomUUID().toString() + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(createSource);

        // Target Table.
        String createSink = "CREATE TABLE print_table (`artist` STRING, `c` BIGINT NOT NULL) " +
                "WITH ('connector' = 'print') ";
        tableEnv.executeSql(createSink);

        String sql = "INSERT INTO print_table SELECT artist, count(*) AS c from sourceTopic WHERE artist <> '' GROUP BY artist";
        tableEnv.executeSql(sql).collect();

        env.execute("KafkaRead");
    }
}