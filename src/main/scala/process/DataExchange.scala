package process
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import util.CommonUtils._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.hive.HiveContext
import readOrwriteData.ReadHiveData
/**
  * suling
  * 数据变换
  * （1）	将同一个用户的所有轨迹点串成一条轨迹
（2）	以记录中出现的网页个数创建数组，将用户对轨迹点的兴趣度写入轨迹位置对应的数组位置，数组位置没有对应轨迹的记为0
  用户对某网页的兴趣度计算公式：intest(l.url)=m-l+1,其中m为网页个数，l为该网页在该用户轨迹中的位置，思想是用户越先访问的网页，兴趣度越高
  由于网页数太多，所以计算时使用的是用户访问的最多次数做为网页个数

  */
object DataExchange {
  def concat_url(data:DataFrame,col:String):DataFrame={
    //将同一个用户的所有轨迹点串成一条轨迹
    data.groupBy(userid).agg(concat_ws(",", collect_set(col)) as "fullurls").select(userid,"fullurls")
  }
  def intest(m:Int,l:Int):Int={
    //计算兴趣度
    return m-l+1
  }
  def getArray(arr:Seq[String],m:Int):Seq[Double]={
    //不定长轨迹转换为定长轨迹
    var intestArray = new Array[Double](m)
    for(i<- 0 until arr.length; if i<m){
      intestArray(i)=intest(m,i).toDouble
    }
    return intestArray.toSeq
  }

  def exchange(data:DataFrame,m:Int):DataFrame={
    //将所有的轨迹分割，计算兴趣度，并转换为定长事务
    //注册自定义函数
    import org.apache.spark.ml.linalg.Vectors

    getSparkSession().udf.register("udf_getArray",(arr:Seq[String],m:Int)=> Vectors.dense(getArray(arr,m).toArray))//数组长度

    getSparkSession().udf.register("udf_getArray_String",(arr:Seq[String],m:Int)=> getArray(arr,m).toArray.mkString(","))
    data.withColumn("furls_arr",split(col("fullurls"),","))
      .selectExpr(userid," fullurls","udf_getArray(furls_arr,"+m+") as "+features,"udf_getArray_String(furls_arr,"+m+") as features_string")
  }
  def dataExchange(data:DataFrame,m:Int)={

    val fullurl_id=data.select("fullurl").distinct().rdd.map(x=>x(0).toString).zipWithIndex.map(x=>Row(x._1.toString,x._2.toString))
    val schema = StructType(Array(StructField("fullurl", StringType, true), StructField("id", StringType, true)))
    val fullurl_id_df=getSparkSession().createDataFrame(fullurl_id,schema)
    fullurl_id_df.registerTempTable("fullurl_id_df")
    //val sparkSession = SparkSession.builder().appName("HiveCaseJob").master("local[*]").enableHiveSupport().getOrCreate()
    //val hiveContext = new HiveContext(sparkSession.sparkContext)
    //hiveContext.sql("create table law_init.fullurl_id1 as select * from fullurl_id_df")
    WriteDB.writeData(fullurl_id_df,fullurlWithid)
    val data_concat=concat_url(data.join(fullurl_id_df,fullUrl),"id")
    val data_exchange=exchange(data_concat,m)
    data_exchange
  }

  def main(args: Array[String]): Unit = {
    //val data=ReadHiveData.readData()
    val data=ReadDB.getData()
    val data_clean=DataClean.clean(data)
    val data_specification=AttributeSpecification.specificate(data_clean,200)
    val data_exchange=dataExchange(data_specification,28)
    data_exchange.show(10,false)
  }
}
