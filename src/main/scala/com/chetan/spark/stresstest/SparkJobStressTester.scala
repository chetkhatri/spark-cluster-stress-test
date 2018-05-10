package com.chetan.spark.stresstest
/*
@Author: Chetan Khatri, Contributor: Apache Spark, Apache HBase
 */
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import com.typesafe.config._
import org.slf4j.LoggerFactory
object SparkJobStressTester {
  val APP_NAME: String = "spark-job-stress-tester"
  var CONFIG_ENV: Config = null
  var ZIP_FILE_PATH: Option[String] = None
  def main(args: Array[String]) = {
    CONFIG_ENV = ConfigFactory.load("configuration.conf")
    ZIP_FILE_PATH = Some(CONFIG_ENV.getString(s"${APP_NAME}.zip-data-file-path"))
    val localZipFile = new File("/tmp/ml-latest.zip")
    FileUtils.copyURLToFile(new URL(ZIP_FILE_PATH.get), localZipFile)
    FileUtils.moveFile(new File("file:/tmp/ml-latest.zip"), new File("hdfs:/tmp/sample_zip/ml-latest.zip"))


  }
}
