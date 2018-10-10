package process
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import readOrwriteData.ReadHiveData
import util.CommonUtils._
/**
  * suling
  * 属性规约
  * 1.	选择用户ID、网址、时间三个字段的数据
2.	按照时间对每个用户的数据进行排序
3.	用户记录超过两百次的删除

  */
object AttributeSpecification {
  def specificate(data:DataFrame,num:Int):DataFrame={
    val data_select=data.select(userid,fullUrl,timestamp_format).orderBy(timestamp_format)
    data_select.registerTempTable("tmp")
    getSparkSession().sql("select temp.userid,temp.fullurl,temp.timestamp_format from (select userid,fullurl,timestamp_format,row_number() over(PARTITION BY userid order by timestamp_format) users from tmp) temp where temp.users<="+num)
  }

  def main(args: Array[String]): Unit = {
    val data=ReadHiveData.readData()
    val cleanData=DataClean.clean(data)
    val specificationData=specificate(cleanData,200)
    specificationData.show(10,false)
  }
}
