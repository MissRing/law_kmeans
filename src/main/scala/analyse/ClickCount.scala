package analyse
import org.apache.spark.sql.DataFrame
import util.CommonUtils._

import scala.collection.mutable
import org.apache.spark.sql.functions._
import readOrwriteData.ReadHiveData

/**
  * suling
  * 用于统计用户点击次数大于几的用户有哪些，记录有多少
  */
object ClickCount {
  def getUserId(data:DataFrame,num:Int):DataFrame={
    data.groupBy("userid").agg(count(userid) as "ucount").filter("ucount>"+num).select("userid").distinct()
  }

  def joinData(data:DataFrame,useridData:DataFrame)={
    data.join(useridData,"userid")
  }
  def main(args: Array[String]): Unit = {
    val data=ReadHiveData.readData()
    val u1=getUserId(data,3)
    val u2=getUserId(data,4)
    val u3=getUserId(data,5)
    //u1.show(20)
    val data_u1=joinData(data,u1)
    val data_u2=joinData(data,u2)
    val data_u3=joinData(data,u3)
    data_u1.show(20)
    println(data_u1.count())
    println(data_u2.count())
    println(data_u3.count())

  }
}
