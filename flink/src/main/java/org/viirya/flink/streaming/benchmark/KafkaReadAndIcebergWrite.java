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

public class KafkaReadAndIcebergWrite {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 1) {
            System.out.println("\nUsage: KafkaReadAndIcebergWrite --bootstrap.servers <kafka brokers> --hadoop.path <catalog path> --iceberg.table <table name> --parallelism <parallelism>");
            return;
        }

        String hadoopPath = params.getRequired("hadoop.path");
        String icebergTable = params.getRequired("iceberg.table");

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

        // Source Table 1.
        String createSource1 = "CREATE TABLE sourceTopic1 (\n" +
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
                "  `method` STRING,\n" +
                "  `page` STRING,\n" +
                "  `registration` BIGINT,\n" +
                "  `sessionId` BIGINT,\n" +
                "  `song` STRING,\n" +
                "  `state` STRING,\n" +
                "  `status` BIGINT,\n" +
                "  `ts` BIGINT,\n" +
                "  `userAgent` STRING,\n" +
                "  `userId` BIGINT,\n" +
                "  `zip` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'page_view_events',\n" +
                "  'properties.bootstrap.servers' = '" + params.getRequired("bootstrap.servers") + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(createSource1);

        String createSource2 = "CREATE TABLE sourceTopic2 (\n" +
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
        tableEnv.executeSql(createSource2);

        String createHadoopCatalog = "CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='" + hadoopPath + "',\n" +
                "  'property-version'='1'\n" +
                ")";
        tableEnv.executeSql(createHadoopCatalog);

        String createIcebergTable = "CREATE TABLE hadoop_catalog.default." + icebergTable + " (\n" +
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
                "  `method` STRING,\n" +
                "  `page` STRING,\n" +
                "  `registration` BIGINT,\n" +
                "  `sessionId` BIGINT,\n" +
                "  `song` STRING,\n" +
                "  `state` STRING,\n" +
                "  `status` BIGINT,\n" +
                "  `ts` BIGINT,\n" +
                "  `userAgent` STRING,\n" +
                "  `userId` BIGINT,\n" +
                "  `zip` STRING\n" +
                ")";
        tableEnv.executeSql(createIcebergTable);

        String sql = "INSERT INTO hadoop_catalog.default." + icebergTable + " SELECT a.* from sourceTopic1 AS a JOIN sourceTopic2 AS b ON a.userId = b.userId AND a.artist = b.artist";
        tableEnv.executeSql(sql).collect();

        env.execute("KafkaReadAndIcebergWrite");
    }
}