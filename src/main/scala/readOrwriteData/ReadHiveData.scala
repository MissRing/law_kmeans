package readOrwriteData
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.CommonUtils._

/**
  * suling
  * 读取Hive表中的数据
  */
object ReadHiveData {

  def readData():DataFrame={
    val sparkSession = SparkSession.builder().appName("HiveCaseJob").master("local[*]").enableHiveSupport().getOrCreate()
    val hiveContext = new HiveContext(sparkSession.sparkContext)
    hiveContext.sql("select * from "+inputTable)

  }

  def main(args: Array[String]): Unit = {
    val data=readData()
    data.show(20)
    data.select(fullUrl).distinct()
    data.select(userid).distinct()
  }
}
