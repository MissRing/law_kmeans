package model

import model.Recommend._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{collect_set, concat_ws, count}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import process.DataClean
import process.DataExchange.getArray
import readOrwriteData.ReadHiveData
import util.CommonUtils.{fullUrl, getSparkSession, userid}
import util.CommonUtils._
/**
  * suling
  * （1）	模型的评估主要是看看推荐的效果如何，现在需要找出一批数据用于测试，这部分数据可以从原先的内容中随意抽取一部分，该部分的数据需要进行筛选，选出访问次数超过20的用户记录。
  * （2）	将其中的前10条抽出作为测试数据
  * （3）	前10条进行预测分类后，每个用户都会得到推荐列表
  * （4）	比较推荐的内容与用户后续访问的内容，出现过为1，未出现为0，最后统计所有的正确推荐数，与总推荐数相除，得到准确率。
  *
  */
object ExecutorModel {
  //准备test，从原先清理好的数据中随意选出访问次数大于10的5%的用户数据，用户记录进行清理，每个用户选择前10个访问路劲作为测试轨迹，剩余部分用于结果的评价。
  def getTest(data:DataFrame,num:Int)={
    val fullurl_gt_num=data.groupBy(fullUrl).agg(count(fullUrl) as "fcount").filter("fcount>"+num).select(fullUrl).distinct()
    val Array(testuser,valibuser)=fullurl_gt_num.randomSplit(Array(0.05,0.1))
    val fullurl_number=getSparkSession().sql("select * from "+fullurlWithid)
    val data_num_gt_ten = data.join(testuser,"userid").join(fullurl_number ,"fullurl")
    val test=data_num_gt_ten.groupBy(userid).agg(concat_ws(",", collect_set("id")) as "fullurls").select(userid,"fullurls").rdd.map{x=>val furl=x(1).toString.split(",");Row(x(0).toString,furl.slice(0,10),furl.slice(10,furl.length))}
    val schema= StructType(Array(StructField("userid", StringType, true), StructField("furls_arr", StringType, true),StructField("valib_arr", StringType, true)))
    val testData=getSparkSession().createDataFrame(test,schema)
    getSparkSession().udf.register("udf_getArray",(arr:Seq[String],m:Int)=> Vectors.dense(getArray(arr,m).toArray))//数组长度
    val test_id= testData.selectExpr("userid","furls_arr","valib_arr" ,"udf_getArray(furls_arr,200) as features")
    test_id
  }

  def predictModel(data:DataFrame)={
    //预测
    val model=KMeansModel.load(modelPath)
    val predict=model.transform(data)
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ttt"))
    val kmeans_center= sc.textFile(centerPath).map{x=>val line=x.slice(1,x.length-1).split(",");Row(line(0),line.slice(1,line.length))}
    val schema2= StructType(Array(StructField("prediction", StringType, true), StructField("cluster_url", StringType, true)))
    val kmeans_centers=getSparkSession().createDataFrame(kmeans_center,schema2)
    val pre_center=predict.join(kmeans_centers,"prediction")
    pre_center
  }


  //评估
  //定义一个求交集的udf
  def getIntersect(a:Seq[String],b:Seq[String]):Seq[String]={
    val in=a intersect b
    return in.toSeq
  }
  //定义一个求差集的udf
  def getDiff(a:Seq[String],b:Seq[String]):Seq[String]={
    val di=a diff b
    return di.toSeq
  }
  //注册函数
  def registerFun()={
    //注册函数
    getSparkSession().udf.register("getIntersect",(a:Seq[String],b:Seq[String])=> getIntersect(a,b))
    getSparkSession().udf.register("getDiff",(a:Seq[String],b:Seq[String])=> getDiff (a,b))
    getSparkSession().udf.register("getLen",(a:Seq[String])=> getLen (a))
  }


  def getLen(a:Seq[String]):Int={
    return a.length
  }
  def executor(pre_center:DataFrame)={
    registerFun()
    val recommend=pre_center.selectExpr("userid","getIntersect(valib_arr, cluster_url) as zhengquetuijian","getDiff(cluster_url,furls_arr) as ketuijian")
    val accuary=recommend.selectExpr("sum(getLen(zhengquetuijian)) as good"," sum(getLen(ketuijian)) as allurl").rdd.map(x=>x(0).toString.toDouble/x(1).toString.toDouble*100)
    (recommend,accuary)
  }

  def main(args: Array[String]): Unit = {
    val data=ReadHiveData.readData()
    val data_clean=DataClean.clean(data)
    val testData=getTest(data_clean,10)
    val predict=predictModel(testData)
    val (recommend,accuary)=executor(predict)
    println(accuary)
  }

}
