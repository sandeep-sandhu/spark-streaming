/**
  * file: StreamAnalyticsApp.scala

  * Purpose: Implements a streaming analytics application
  * using the Apache spark structured streaming platform.

  * References:
  * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  * https://jaceklaskowski.gitbooks.io/spark-streaming/content/spark-streaming-kafka-KafkaUtils.html
  *

cd %USERPROFILE%\Documents\src\spark_projs\stream_analytics

  * source files should be placed under directory main/scala/ for sbt to pick up:
sbt package

  * https://www.tutorialspoint.com/apache_kafka/apache_kafka_integration_spark.htm
  * Start Kafka Producer CLI (explained in the previous chapter):
  * create a new topic called "my-first-topic" and provide some sample messages as shown below.

  * program to run:
spark-submit --master local[*] --files %~dp0\src\main\resources\application.conf,%~dp0\src\main\resources\log4j.properties --driver-java-options "-Dconfig.file=src/main/resources/application.conf -Dlog4j.configurationFile=file:/src/main/resources/log4j.properties " target/scala-2.13/StreamAnalyticsApp-assembly-1.0.jar --broker localhost:9092 --topic prod_stream1


  *
  */

package io.github.sandeep_sandhu.stream_analytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  StringType,
  StructType
}
import org.apache.spark.sql.functions.{col, udf, _}

import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.log4j.{Level, Logger}
import com.esotericsoftware.kryo.Kryo
import scala.annotation.tailrec
import scala.collection._

case class ModelDataRecord(
  ID: Int,
  AMOUNT: Double,
  REMARKS: String,
  TARGET_LABEL: String
)

object StreamAnalyticsApp {

  val appName = "StreamAnalyticsApp"

  private val usage =
    """Usage: StreamAnalyticsApp --broker <kafkaserver> --topic <topicname>
      """

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger
      .getLogger("org.apache.spark.storage.BlockManager")
      .setLevel(Level.ERROR)
    val logger: Logger = Logger.getLogger(appName)
    logger.setLevel(Level.INFO)

    if (args.length == 0) {
      println(usage)
      logger.error("All necessary arguments not provided.")
      System.exit(1)
    }
    logger.info(s"Started the application: $appName.")

    val cmdArgOptions: Map[String, String] = this.nextArg(Map(), args.toList)

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.default.parallelism", "6")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("setWarnUnregisteredClasses", "true")
      //.set("spark.kryo.registrationRequired", "true") // <- disabled because of OpenHashMap errors when fitting models
      .registerKryoClasses(prepareListOfKryoClasses())

    //create a spark session from the configuration
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Subscribe to 1 topic, with headers
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cmdArgOptions("broker"))
      .option("subscribe", cmdArgOptions("topic"))
      .option("includeHeaders", "true")
      .load()

    logger.info("Dataframe is of type datastream? " + kafkaStreamDF.isStreaming)

    kafkaStreamDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
      .as[(String, String, Array[(String, Array[Byte])])]

//    val stream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )
//      stream.map( record => (record.key, record.value))

    // Split the lines into words
    val words = kafkaStreamDF.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

    spark.stop

  }

  def prepareListOfKryoClasses(): Array[Class[_]] =
    Array(
      classOf[ModelDataRecord],
      Class.forName(
        "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"
      ),
      Class.forName("org.apache.spark.sql.types.StringType$"),
      Class.forName("org.apache.spark.sql.types.IntegerType$"),
      Class.forName("org.apache.spark.sql.types.ByteType$"),
      Class.forName("org.apache.spark.sql.types.DoubleType$"),
      Class.forName("org.apache.spark.sql.types.BooleanType$"),
      Class.forName("org.apache.spark.ml.linalg.VectorUDT"),
      Class.forName("org.apache.spark.ml.linalg.MatrixUDT"),
      classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
      classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
      classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
      classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
      classOf[org.apache.spark.sql.types.Metadata],
      classOf[Array[org.apache.spark.sql.types.StructType]],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[Array[org.apache.spark.sql.types.StructField]],
      classOf[org.apache.spark.sql.types.StructField],
      classOf[org.apache.spark.sql.types.StringType],
      classOf[org.apache.spark.sql.types.ArrayType]
    )

  /**
    * Print all of Spark's configuration parameters
    * @param config: SparkContext of the current spark session
    */
  private def getSparkConfigParams(config: SparkConf): String = {
    val strBuilder = new StringBuilder()
    // for each key-value pair, prepare a string key=value, append it to the string builder
    for ((k: String, v: String) <- config.getAll) {
      strBuilder ++= s"\n$k = $v"
    }
    // return prepared string
    strBuilder.mkString
  }

  /**
    * Parse the next command line argument, recursively building up the map.
    * @param map The hashmap in which all switches and their values are stored as key-value pairs
    * @param list The command line arguments passed as a list
    * @return
    */
  @tailrec
  def nextArg(
    map: Map[String, String],
    list: List[String]
  ): Map[String, String] =
    list match {
      case Nil => map
      case "--broker" :: value :: tail =>
        nextArg(map ++ Map("broker" -> value.toLowerCase()), tail)
      case "--topic" :: value :: tail =>
        nextArg(map ++ Map("topic" -> value.toLowerCase()), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        map
    }
}
