package analyse
import org.apache.spark.sql.DataFrame
import util.CommonUtils._
import org.apache.spark.sql.functions._
import readOrwriteData.ReadHiveData
/**
  * suling
  * 网址相关探索
  */
object FullUrlAnalyse {

  def countFullurl(data:DataFrame)={
    //网页数探索
    data.select(fullUrl).distinct().count()
  }
  def countClickURLUser(data:DataFrame)={
    //各网页访问用户数统计
    data.groupBy(fullUrl).agg(count(fullUrl) as "urlcount",count(fullUrl)*100/25358006.0 as "url_per").select(col(fullUrl),col("urlcount"),col("url_per")).orderBy(desc("urlcount"))
  }

  def getUrlLow10(data:DataFrame,num:Int)={
    //被访问次数低于10的网页
    data.filter("urlcount<="+num)
  }
  def countUserClickUrl(data:DataFrame)={
    //每个用户访问的网页数统计
    data.groupBy("userid").agg(count(fullUrl) as "fcount",countDistinct(fullUrl) as "fcount_dis")
      .select(col(userid),col("fcount"),col("fcount_fis")).groupBy(col("fcount_dis"),col("fcount"))
      .agg(count(userid) as "ucount",count(userid)*100.0/3617090 as "user_per").orderBy(desc("ucount"))
  }


  def main(args: Array[String]): Unit = {
    val data=ReadHiveData.readData()
    val urlcount=countFullurl(data)
    println(urlcount)
    val clickurlUser=countClickURLUser(data)
    clickurlUser.show(1000,false)
    //统计访问次数在10以内的网页数
    val web_low10=getUrlLow10(clickurlUser,10)
    println("被访问次数低于10的网页有："+web_low10.count())
    //统计用户访问的网页数中各个网页数的占比
    val userclickUrlCount=countClickURLUser(data)
    userclickUrlCount.show(10,false)
  }
}
