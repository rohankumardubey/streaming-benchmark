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

public class KafkaFilter {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 1) {
            System.out.println("\nUsage: KafkaFilter --bootstrap.servers <kafka brokers> --parallelism <parallelism>");
            return;
        }

        Properties kparams = params.getProperties();
        kparams.setProperty("auto.offset.reset", "latest");
        kparams.setProperty("flink.starting-position", "latest");
        kparams.setProperty("group.id", UUID.randomUUID().toString());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(3000000); // 3000 seconds
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(Integer.parseInt(params.getRequired("parallelism")));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String groupId = UUID.randomUUID().toString();

        String createSource = "CREATE TABLE sourceTopic (\n" +
                "  `artist` STRING,\n" +
                "  `auth` STRING,\n" +
                "  `city` STRING,\n" +
                "  `duration` DOUBLE,\n" +
                "  `firstName` STRING,\n" +
                "  `gender` STRING,\n" +
                "  `itemInSession` BIGINT,\n" +
                "  `lastName` STRING,\n" +
                "  `lat` DOUBLE,\n" +
                "  `level` STRING,\n" +
                "  `lon` DOUBLE,\n" +
                "  `registration` BIGINT,\n" +
                "  `sessionId` BIGINT,\n" +
                "  `song` STRING,\n" +
                "  `state` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `userAgent` STRING,\n" +
                "  `userId` BIGINT,\n" +
                "  `zip` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'listen_events',\n" +
                "  'properties.bootstrap.servers' = '" + params.getRequired("bootstrap.servers") + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(createSource);

        // Target Table.
        String createSink = "CREATE TABLE targetTopic (\n" +
                "  `artist` STRING,\n" +
                "  `auth` STRING,\n" +
                "  `city` STRING,\n" +
                "  `duration` DOUBLE,\n" +
                "  `firstName` STRING,\n" +
                "  `gender` STRING,\n" +
                "  `itemInSession` BIGINT,\n" +
                "  `lastName` STRING,\n" +
                "  `lat` DOUBLE,\n" +
                "  `level` STRING,\n" +
                "  `lon` DOUBLE,\n" +
                "  `registration` BIGINT,\n" +
                "  `sessionId` BIGINT,\n" +
                "  `song` STRING,\n" +
                "  `state` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `userAgent` STRING,\n" +
                "  `userId` BIGINT,\n" +
                "  `zip` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'benchmark_test',\n" +
                "  'properties.bootstrap.servers' = '" + params.getRequired("bootstrap.servers") + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(createSink);

        String sql = "INSERT INTO targetTopic SELECT a.* from sourceTopic a WHERE a.duration > 200.0";
        tableEnv.executeSql(sql).collect();

        env.execute("KafkaFilter");
    }
}