/**
  * StructuredStreamSpec:
  *
  * Run coverage report with sbt using command:
  * sbt ';coverageEnabled;test'
  */
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome}
import org.scalatest.funsuite.AnyFunSuite

import io.github.sandeep_sandhu.stream_analytics._

// Enables setting up fixtures using before-all and after-all
class StructuredStreamSpec
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  self =>
  @transient var ss: SparkSession = null
  @transient var sc: SparkContext = null

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.ss.sqlContext
  }

  override def beforeAll(): Unit = {
    val sparkConfig = new SparkConf().setAppName("Structured Streaming Test")
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.sql.shuffle.partitions", "4")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.master", "local")

    ss = SparkSession.builder().config(sparkConfig).getOrCreate()

  }

  override def afterAll(): Unit =
    ss.stop()

  test("A basic dataframe creation test") {

    val df = ss
      .createDataFrame(
        Seq((1, "a string", "another string", 12344567L))
      )
      .toDF(
        "first val",
        "stringval",
        "stringval2",
        "longnum"
      )

    assert(df.count == 1)
  }

}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session for scala tests")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
