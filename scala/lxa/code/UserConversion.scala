package lxa.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, not}

import java.util.Properties

object UserConversion {
  def main(): Unit = {
    val tool = new Tool()
    val spark = SparkSession.builder()
      .appName("UserConversion")
      .master("local[*]")
      .getOrCreate()

    val actionDF = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv(tool.INPUTTPATH)

    val userconv = actionDF.filter("event_type is not null")
      .groupBy("event_type")
      .count()
      .filter(not(col("event_type").contains("remove_from_cart")))

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    userconv.write.mode("overwrite").option("truncate", true)
      .jdbc(tool.SQLPATH, "userconv", prop)
  }
}
