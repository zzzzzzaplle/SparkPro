package main.scala.lxa.code
import lxa.code.Tool
import org.apache.spark.sql.functions.{col, desc, isnull}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

//11月品牌销量前五
object BrandTop5 {
  def main(): Unit = {
    val tool = new Tool()
    val spark = SparkSession.builder()
      .appName("BrandTop5")
      .master("local[*]")
      .getOrCreate()

    val actionDF = spark.read
      .option("header",true)
      .option("delimiter",",")
      .csv(tool.INPUTTPATH )


    val Top5Res = actionDF
      .filter("brand is not null")
      .filter(col("event_type").contains("purchase"))
      .groupBy("brand")
      .count()
      .orderBy(desc("count"))
      .limit(5)

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    Top5Res.write.mode("overwrite").option("truncate", true)
      .jdbc(tool.SQLPATH, "topbrtand5", prop)


  }
}
