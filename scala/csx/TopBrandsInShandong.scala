package com.niit.Pro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, sum}

import java.util.Properties

//山东省销量前五的品牌及其销量

object TopBrandsInShandong {
  def main(): Unit = {
    val spark = SparkSession.builder()
      .appName("TopBrandsInShandong")
      .master("local[*]")
      .getOrCreate()

    val salesDF = spark.read.option("header", "true").option("delimiter", "\t").csv("inputdata/csx/data.csv")

    val headerSales = salesDF.first()
    val filteredSalesDF = salesDF.filter(row => row != headerSales)

    val dfWithSaleQtty = filteredSalesDF.withColumn("sale_qtty", col("sale_qtty").cast("int"))

    val cityDF = spark.read.option("header", "true").option("delimiter", ",").option("encoding", "GBK").csv("inputdata/csx/city_level.csv")

    val headerCity = cityDF.first()
    val filteredCityDF = cityDF.filter(row => row != headerCity)

    dfWithSaleQtty.createOrReplaceTempView("sales")
    filteredCityDF.createOrReplaceTempView("city")

    val topBrandsDF = dfWithSaleQtty.join(filteredCityDF, dfWithSaleQtty("user_site_province_id") === filteredCityDF("dim_province_id"))
      .filter(col("dim_province_name") === "山东")
      .groupBy("brandname")
      .agg(sum("sale_qtty").alias("total_sales"))
      .orderBy(desc("total_sales"))
      .limit(5)

    topBrandsDF.show()

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    topBrandsDF.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","TopBrandInShandong",prop)


    spark.stop()
  }


}
