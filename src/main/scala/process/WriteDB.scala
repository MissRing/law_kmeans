package process

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}
import readOrwriteData.ReadHiveData
import util.CommonUtils._

/**
  * suling
  * 写入数据到mysql
  *
  */
object WriteDB {
  def writeData(data:DataFrame,table:String)={
    val url = "jdbc:mysql://localhost:3306/law_init?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "root"

    val driver ="com.mysql.jdbc.Driver"
    val  properties = new Properties()
    properties.put("user",user)
    properties.put("password",password)
    properties.put("driver",driver)
    data.write.mode(SaveMode.Append).jdbc(url,table,properties)

  }

  def main(args: Array[String]): Unit = {
    val data=ReadHiveData.readData()
    val data_hunyin=OnlyHunyin.getData(data,"info/hunyin")
    writeData(data_hunyin,"law_init.lawtime_hunyin")
  }
}
