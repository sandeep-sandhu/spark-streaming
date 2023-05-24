name := "Stream Analytics"

version := "0.1"

scalaVersion := "2.13.10"

enablePlugins(AssemblyPlugin)

assembly / mainClass := Some("StreamAnalyticsApp")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided"
  , "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
  , "org.apache.spark" %% "spark-streaming" % "3.4.0" % "provided"
  , "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.0" % "provided"
  , "org.apache.spark" %% "spark-graphx"  % "3.4.0" % "provided"
  , "org.apache.spark" %% "spark-mllib"  % "3.4.0" % "provided"
  , "log4j" % "log4j" % "1.2.17" % "provided"
)

// avoid adding scala version as suffix by using single % after org name:

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.3.2"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
//libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.3.2"


// for testing:
coverageEnabled := true

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// config file reader:
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

scalacOptions ++= Seq(
  //  "-target:jvm-1.11",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)

