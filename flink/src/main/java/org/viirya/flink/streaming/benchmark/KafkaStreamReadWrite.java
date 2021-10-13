package org.viirya.flink.streaming.benchmark;

import java.util.Properties;
import java.util.UUID;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.Rowtime;

public class KafkaStreamReadWrite {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		if (params.getNumberOfParameters() < 1) {
			System.out.println("\nUsage: KafkaStreamReadWrite --bootstrap.servers <kafka brokers> --topic.read <topic> --topic.write <topic> --parallelism <parallelism>");
			return;
		}

		Properties kparams = params.getProperties();
		kparams.setProperty("auto.offset.reset", "earliest");
		kparams.setProperty("flink.starting-position", "earliest");
		kparams.setProperty("group.id", UUID.randomUUID().toString());
		kparams.setProperty("bootstrap.servers", params.getRequired("bootstrap.servers"));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(3000000); // 3000 seconds
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(Integer.parseInt(params.getRequired("parallelism")));

		DataStream<String> stream = env
						.addSource(new FlinkKafkaConsumer<>(params.getRequired("topic.read"), new SimpleStringSchema(), kparams));
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		Properties writerParams = params.getProperties();
		writerParams.setProperty("bootstrap.servers", params.getRequired("bootstrap.servers"));

		FlinkKafkaProducer<String> testProducer = new FlinkKafkaProducer<>(
						params.getRequired("bootstrap.servers"),
						params.getRequired("topic.write"),
						new SimpleStringSchema());

		stream.addSink(testProducer);
		env.execute("KafkaStreamReadWrite");
	}
}