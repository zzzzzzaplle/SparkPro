package com.niit.Pro

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, expr, round}

import java.util.Properties

object BrandData {
  def main(): Unit = {
    val spark = SparkSession.builder()
      .appName("BrandData")
      .master("local[*]")
      .getOrCreate()


//    src/main/scala/csx/PJ/data.csv
    val df = spark.read.option("header", "true").option("delimiter", "\t").csv("inputdata/csx/data.csv")
//    val df = spark.read.option("header", "true").option("delimiter", "\t").csv("src/main/scala/csx/PJ/data.csv")

    val header = df.first()
    val filteredDF = df.filter(row => row != header)

    val dfWithPrice = filteredDF.withColumn("after_prefr_unit_price", col("after_prefr_unit_price").cast("double"))

    // 过滤掉after_prefr_unit_price为0的行
    val nonZeroPriceDF = dfWithPrice.filter(col("after_prefr_unit_price") =!= 0)


    val priceStatsDF = nonZeroPriceDF.groupBy("brandname")
      .agg(
        round(avg("after_prefr_unit_price"), 2).alias("avg_price")
//        expr("percentile_approx(after_prefr_unit_price, 0.5)").alias("median_price"),
//        functions.max("after_prefr_unit_price").alias("max_price"),
//        functions.min("after_prefr_unit_price").alias("min_price")
      )

    priceStatsDF.show()

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    priceStatsDF.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","BrandData",prop)


    spark.stop()
  }

}
