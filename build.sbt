ThisBuild / name         := "stream-benchmark"
ThisBuild / version      := "0.1"
ThisBuild / scalaVersion := "2.12.10"

lazy val spark = project
  .settings(
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0",
    libraryDependencies ++= {
      Seq(
        "org.apache.spark"              %%  "spark-sql"                   % "3.0.1"       % "provided",
        "org.apache.spark"              %%  "spark-sql-kafka-0-10"        % "3.0.1"       % "provided",
        "org.apache.spark"              %%  "spark-streaming-kafka-0-10"  % "3.0.1"       % "provided"
      )
    }
  )

lazy val flink = project
  .settings(
    libraryDependencies ++= {
      Seq(
        "org.apache.kafka"              %  "kafka-clients"                % "2.4.1"        % "provided",
        "org.apache.flink"              %  "flink-java"                   % "1.12.1"       % "provided",
        "org.apache.flink"              %  "flink-json"                   % "1.12.1"       % "provided",
        "org.apache.flink"              %  "flink-table"                  % "1.12.1"       % "provided",
        "org.apache.flink"              %  "flink-table-api-java"         % "1.12.1"       % "provided",
        "org.apache.flink"              %% "flink-table-api-java-bridge"  % "1.12.1"       % "provided",
        "org.apache.flink"              %%  "flink-streaming-java"        % "1.12.1"       % "provided",
        "org.apache.flink"              %%  "flink-streaming-scala"       % "1.12.1"       % "provided",
        "org.apache.flink"              %%  "flink-clients"               % "1.12.1"       % "provided",
        "org.apache.flink"              %%  "flink-connector-kafka"       % "1.12.1"       % "provided",
      )
    }
  )

mergeStrategy in assembly ~= ( (old) => {
    case PathList("module-info.class" ) => MergeStrategy.discard
    case PathList("javax", "xml", "bind", ps @ _*) => MergeStrategy.first
    case x => old(x)
  }
)
