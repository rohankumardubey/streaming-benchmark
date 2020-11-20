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
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic(params.getRequired("read-topic"))
                .property("bootstrap.servers", params.getRequired("bootstrap.servers")))
                .withSchema(new Schema()
                        .field("artist", Types.STRING())
                        .field("auth", Types.STRING())
                        .field("city", Types.STRING())
                )
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .createTemporaryTable("sourceTopic");

        String sql = "SELECT * from sourceTopic";
        tableEnv.executeSql(sql).print();

        env.execute("KafkaRead");
    }
}