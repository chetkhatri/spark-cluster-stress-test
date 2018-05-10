package com.chetan.spark.stresstest
/*
@Author Chetan
 */
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.broadcast
import org.slf4j.LoggerFactory
object SampleSparkJob {
  val APP_NAME: String = "spark-job-stress-tester"
  val logger = LoggerFactory.getLogger(getClass.getName)
  var CONFIG_ENV: Config = null
  var GENOME_SCORES_CSV_PATH: Option[String] = None
  var GENOME_TAGS_CSV_PATH: Option[String] = None
  var LINKS_CSV_PATH: Option[String] = None
  var MOVIES_CSV_PATH: Option[String] = None
  var RATINGS_CSV_PATH: Option[String] = None
  var TAGS_CSV_PATH: Option[String] = None
  def main(args: Array[String]): Unit = {
    CONFIG_ENV = ConfigFactory.load("configuration.conf")
    val GENOME_SCORES_CSV_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.genome-scores-csv-path"))
    val GENOME_TAGS_CSV_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.genome-tags-csv-path"))
    val LINKS_CSV_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.links-csv-path"))
    val MOVIES_CSV_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.movies-csv-path"))
    val RATINGS_CSV_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.ratings-csv-path"))
    val TAGS_CSV_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.tags-csv-path"))

    val spark = SparkSession.builder().appName(APP_NAME).getOrCreate()
    import spark.implicits._
    val genomeScoresDS = spark.read.option("header", "true").csv(GENOME_SCORES_CSV_PATH.get).toDF()
    val genomeTagsDS = spark.read.option("header", "true").csv(GENOME_TAGS_CSV_PATH.get).toDF()
    val finalDS = genomeScoresDS.join(broadcast(genomeTagsDS), genomeScoresDS.col("tagId") === genomeTagsDS.col("tagId")).drop(genomeTagsDS.col("tagId"))
    logger.info("################## sample data before you write to HDFS ####################")
    //finalDS.show()
    finalDS.repartition(1).write.mode(SaveMode.Overwrite).parquet("/sample-spark-df/finalDS.parquet")
    finalDS.repartition(1).write.mode(SaveMode.Overwrite).csv("/sample-spark-df/finalDS.csv")

    val finalDFParquet = spark.read.parquet("/sample-spark-df/finalDS.parquet")
    logger.info("######### show finalDFParquet #########")
    finalDFParquet.show()
    val finalDFCSV = spark.read.option("inferSchema", "true").csv("/sample-spark-df/finalDS.csv")
    logger.info("########  show finalDFCSV #########")
    finalDFCSV.show()

  }
}
