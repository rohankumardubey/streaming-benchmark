package org.viirya.spark.streaming.benchmark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

object KafkaSchema {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaSchema <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics, _*) = args
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("KafkaSchema")
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

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 10)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .select(schema_of_json(col("value")))

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }

}
