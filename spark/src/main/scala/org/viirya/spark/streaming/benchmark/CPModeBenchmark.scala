package org.viirya.spark.streaming.benchmark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, to_json}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.Trigger

object CPModeBenchmark {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: CPModeBenchmark <bootstrap-servers> [<trigger interval>] [<checkpoint-location>]")
      System.exit(1)
    }

    val bootstrapServers = args(0)

    val triggerInterval = if (args.length > 1) {
      Some(args(1))
    } else {
      None
    }

    val checkpointLocation =
      if (args.length > 2) args(2) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("CPModeBenchmark")
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

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "listen_events")
      .option("failOnDataLoss", "false")
      .option("minPartitions", "5")

    val listen_events = reader
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"),
        DataSchema.listen_events_dt,
        Map.empty[String, String]).as("listen_events"))

    val filtered = listen_events.filter($"listen_events.duration" > 200.0)
      .select(to_json($"listen_events").as("value"))

    val query = filtered.writeStream
      .outputMode("append")
      .format("kafka")
      .option("checkpointLocation", checkpointLocation)
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", "benchmark_test")

    triggerInterval.foreach(t => query.trigger(Trigger.Continuous(t)))
    query.start().awaitTermination()
  }
}
