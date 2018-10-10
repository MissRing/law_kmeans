package util

import org.apache.spark.sql.SparkSession

/**
  * suling
  * 定义一些常量
  */
object CommonUtils {
  val inputTable="law_init1.lawtime_gt_one_distinct"
  val userid="userid"
  val pageTitle="pagetitle"
  val fullUrl="fullurl"
  val timestamp_format="timestamp_format"
  val features="features"
  val predict_col="prediction"
  val fullurlWithid="law_init.fullurl_id0"
  val modelPath="/user/root/pathModel"
  val centerPath="/user/root/pathCenter"
  val spark = SparkSession
    .builder()
    .appName("Spark ")
      .master("local[2]")
    .getOrCreate()

  def getSparkSession() = spark

}
