package org.viirya.spark.streaming.benchmark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, to_json}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.Trigger

object JoinBenchmark {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: JoinBenchmark <bootstrap-servers> [<max offsets>] [<trigger interval>] [<checkpoint-location>]")
      System.exit(1)
    }

    val bootstrapServers = args(0)
    val maxOffsetsPerTrigger = if (args.length > 1) {
      Some(args(1))
    } else {
      None
    }

    val triggerInterval = if (args.length > 2) {
      Some(args(2))
    } else {
      None
    }

    val checkpointLocation =
      if (args.length > 4) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("KafkaRead")
      .getOrCreate()

    import spark.implicits._

    // Add streaming query listener to check progress.
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    val reader1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "listen_events")
      .option("failOnDataLoss", "false")
      .option("minPartitions", "5")

    maxOffsetsPerTrigger.foreach(reader1.option("maxOffsetsPerTrigger", _))

    val listen_events = reader1
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"),
        DataSchema.listen_events_dt,
        Map.empty[String, String]).as("listen_events"))

    val reader2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "page_view_events")
      .option("failOnDataLoss", "false")
      .option("minPartitions", "5")

    maxOffsetsPerTrigger.foreach(reader2.option("maxOffsetsPerTrigger", _))

    val page_view_events = reader2
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"),
        DataSchema.page_view_events_dt,
        Map.empty[String, String]).as("page_view_events"))

    val join = listen_events.join(page_view_events,
      $"listen_events.userId" === $"page_view_events.userId" &&
        $"listen_events.artist" === $"page_view_events.artist")
      .select(to_json($"listen_events").as("value"))

    val query = join.writeStream
      .outputMode("append")
      .format("kafka")
      .option("checkpointLocation", checkpointLocation)
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", "benchmark_test")

    triggerInterval.foreach(t => query.trigger(Trigger.ProcessingTime(t)))
    query.start().awaitTermination()
  }
}
