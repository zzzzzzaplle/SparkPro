package lxa.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, substring, to_date}
import java.util.Properties

object TransVolByHour {
  def main(): Unit = {

    val tool = new Tool()
    val spark = SparkSession.builder()
      .appName("TransVolByHour")
      .master("local[*]")
      .getOrCreate()

    val actionDF = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv(tool.INPUTTPATH)

    //双十一当天分时交易额
    val res = actionDF.filter(col("event_time").contains("2019-11-12"))
      .withColumn("event_hour",substring(col("event_time"),12,2))
      .filter(col("event_type").contains("purchase"))
      .withColumn("price",col("price").cast("Double"))
      .groupBy("event_hour")
      .sum("price")
      .withColumn("sum(price)",round(col("sum(price)"),2))
      .withColumn("event_hour",col("event_hour").cast("int"))
      .orderBy("event_hour")



    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    res.write.mode("overwrite").option("truncate", true)
      .jdbc(tool.SQLPATH, "trans", prop)

  }
}
