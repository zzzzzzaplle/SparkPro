package lxa.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, datediff, desc, last_day, lit, round, sum, to_date, when}
import java.util.Properties

//用户分群RFM模型
//RFM模型是通过一个客户的近期购买行为、购买的总体频率以及花了多少钱3个维度来描述该客户价值状况的客户分类模型，这3个维度分别表示为：
//最近一次消费距离现在的时间 (Recency)：这个值越小对我们来说价值越大； (以12月1为基准)
//某段时间内消费频率次数 (Frequency)：这个值越大越好
//某段时间内消费金额 (Monetary)：这个值越大越好
object RFM {
  def main(): Unit = {

    val tool = new Tool()
    val spark = SparkSession.builder()
      .appName("BrandTop5")
      .master("local[*]")
      .getOrCreate()

    val actionDF = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv(tool.INPUTTPATH)
      .withColumn("event_time", to_date(col("event_time")))  // 时间内容格式优化
      .withColumn("price",col("price").cast("Double"))    //字段内容优化

    //最后一次购买距离标志时间的长度
    val Recency = actionDF.groupBy("user_id","event_time").count()
      .orderBy(desc("event_time"))    //倒序排列，最后一次购买rank 1
      .dropDuplicates("user_id")        //根据列去重，多个重复值仅保留第一个，目的为保留最后一次购买时间
      .withColumn("sttime",to_date(lit("2019-12-1")))
      .withColumn("R", datediff(col("sttime"),col("event_time")) )
      .drop("sttime","event_time","count")


      //购买频次计算
      val  Frequency = actionDF.filter(col("event_type").contains("purchase"))
        .groupBy("user_id").count()
        .withColumnRenamed("count","F")
        .drop("count")


      //购买金额
      val Monetary = actionDF.filter(col("event_type").contains("purchase"))
        .groupBy("user_id")
        .sum("price")
        .withColumn("M",round(col("sum(price)"),2))
        .drop("sum(price)")


      //更具r*-1+f+m，来获得rfm值，进行用户类别划分
      val RFMRes = Recency.join(Frequency,"user_id")
        .join(Monetary,"user_id")
        .withColumn("t",col("R") * -1 + col("F")+col("M")+100)
        .withColumn("t",round(col("t"),2))
        .orderBy("t")
        .withColumn("RFM",when(col("t") >300,"重要价值用户")
          .when(col("t") >220,"重要发展用户")
          .when(col("t") > 190  , "重要挽留用户")
          .when(col("t") > 160  , "一般价值用户")
          .when(col("t") > 130  , "一般发展用户")
          .when(col("t") > 100  , "一般挽留用户")
          .when(col("t") > 1  , "一般用户"))
        .groupBy("RFM").count()
        .orderBy("RFM")
      //大部分数据在RFM 100 - 200
      //全部数据分布在 50 - 1200


    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    RFMRes.write.mode("overwrite").option("truncate", true)
      .jdbc(tool.SQLPATH, "user_value", prop)


  }
}
