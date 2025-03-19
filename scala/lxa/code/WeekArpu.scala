package lxa.code
import main.scala.lxa.code.BrandTop5
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, to_date}

import java.util.Properties
//周活跃消费ARPU计算对比（双十一所在周及后一周对比）
//-- ARPU = 每日消费总次数 / 每日活跃行为数
object WeekArpu {
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
      .withColumn("ymd_time",to_date(col("event_time")))

    //双十当周前后arpu
    val dayactiontoalDF = actionDF
      .filter(col("ymd_time").between("2019-11-11","2019-11-17"))
      .groupBy(col("ymd_time")).count()
      .withColumnRenamed("count","action_count")


    val daypurchaseDF = actionDF
      .filter(col("ymd_time").between("2019-11-11","2019-11-17"))
      .groupBy(col("ymd_time"),col("event_type")).count()
      .filter(col("event_type").contains("purchase"))
      .withColumnRenamed("count","purchase_count")
      .drop("event_type")

    val weekarpuDF = dayactiontoalDF.join(daypurchaseDF,"ymd_time")
      .withColumn("arpu",round(col("purchase_count")/col("action_count") , 5))
      .drop("action_count","purchase_count")


    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    weekarpuDF.write.mode("overwrite").option("truncate", true)
      .jdbc(tool.SQLPATH, "weekarpu", prop)

  }
}
