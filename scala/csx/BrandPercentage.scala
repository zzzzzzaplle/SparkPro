package com.niit.Pro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, round, sum}

import java.util.Properties

object BrandPercentage {
  def main(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataAnalysis")
      .master("local[*]")
      .getOrCreate()

    val filename = "inputdata/csx/data.csv"
    val df = spark.read.option("header", "true").option("delimiter", "\t").csv(filename)

    val brandCounts = df.groupBy("brandname").agg(sum("sale_qtty").alias("total_sale_qtty"))

    val totalSales = brandCounts.agg(sum("total_sale_qtty").cast("double")).collect()(0).getDouble(0)

    val result = brandCounts.withColumn("percentage", round(col("total_sale_qtty") / totalSales * 100, 2))
      .orderBy(desc("percentage"))
    result.show()

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    result.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","BrandPercentage",prop)

    spark.stop()
  }
}
