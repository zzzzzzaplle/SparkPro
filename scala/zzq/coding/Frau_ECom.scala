package main.scala.zzq.coding
import java.util.Properties
import org.apache.spark.sql.expressions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Frau_ECom {
  def main(): Unit = {

    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("E-commerce Fraud Detection")
      .master("local[*]") // 在实际部署时，应该使用集群的设置
      .getOrCreate()
    // 必要的导入
    spark.conf.set("spark.task.maxFailures", "3")


    import spark.implicits._
    import org.apache.spark.sql.functions._
//    使用 spark.implicits._ 来启用 DataFrame 的隐式转换。
//    导入Spark SQL函数，包括 md5 和 col。
val dataPath = "inputdata/zzq/dataset.csv"
    val df = spark.read
      .option("header", "true") // 使用文件中的头部
      .option("delimiter", ",") // CSV文件的分隔符
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .option("charset", "UTF-8")
      .csv(dataPath)
    println(s"Total number of rows: ${df.count()}") // 打印总行数
// 读取数据

    // 读取数据
    val typedDF = df
      .withColumn("Transaction_Amount", col("Transaction_Amount").cast("double"))
      .withColumn("Quantity", col("Quantity").cast("int"))
      .withColumn("Customer_Age", col("Customer_Age").cast("int"))
      .withColumn("Is_Fraudulent", col("Is_Fraudulent").cast("int"))
      .withColumn("Account_Age_Days", col("Account_Age_Days").cast("int"))
      .withColumn("Transaction_Date", col("Transaction_Date").cast("timestamp"))

    val hashedData =typedDF
      .withColumn("Transaction_ID", substring(md5(col("Transaction_ID")), 0, 8))
      .withColumn("Customer_ID", substring(md5(col("Customer_ID")), 0, 8))
    hashedData.show(10)
    // 基于RDD的分析需求
    // 将DataFrame转换为RDD
   hashedData.columns.foreach(println)
    val rdd = hashedData.rdd

    // 需求1：统计每个年龄段客户的交易数量
    val ageGroups = rdd.map(row => {
      val age = row.getInt(row.fieldIndex("Customer_Age"))
      val ageRange = age match {
        case a if 0 until 19 contains a => "0-18"
        case a if 19 until 36 contains a => "19-35"
        case a if 36 until 52 contains a => "36-51"
        case a if 52 to 100 contains a => "52-100"
        case _ => "unknown"
      }
      (ageRange, 1)
    })
    val countByAgeRange = ageGroups.reduceByKey(_ + _).collect()
    val totalTransactions = countByAgeRange.map(_._2).sum
    val ageRangeWithPercentage = countByAgeRange.map { case (range, count) =>
      val percentage = (count.toDouble / totalTransactions) * 100
      (range, count, f"$percentage%.2f")
    }

    val ageRangeDf = spark.createDataFrame(ageRangeWithPercentage).toDF("ageRange", "count", "percentage")

    // 删除 unknown 行,处理不合适数据
    val filteredAgeRangeDf = ageRangeDf.filter($"ageRange" =!= "unknown")

    // 将 ageRange 列转换为数值进行排序
    val sortedAgeRangeDf = filteredAgeRangeDf.withColumn("ageRangeNum", when($"ageRange" === "0-18", 1)
      .when($"ageRange" === "19-35", 2)
      .when($"ageRange" === "36-51", 3)
      .when($"ageRange" === "52-100", 4))
      .orderBy("ageRangeNum")
      .drop("ageRangeNum")

    println("=== Transaction Count by Age Range (Sorted and Filtered) ===")
//    sortedAgeRangeDf.show()

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")

    sortedAgeRangeDf.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","transaction_count_by_age_range",prop)



    // 需求2统计不同账户时间段的交易数量和欺诈交易数量占比
    // 定义账户年龄段
    val accountAgeBuckets = when(col("Account_Age_Days") <= 100, "0-100")
      .when(col("Account_Age_Days") <= 300, "101-300")
      .otherwise("301+")

    // 添加账户年龄段列
    val dataWithAgeBucket =
      hashedData.withColumn("Account_Age_Bucket", accountAgeBuckets)

    // 统计不同账户年龄段的交易数量和欺诈交易数量
    val transactionCountsByAge = dataWithAgeBucket.groupBy("Account_Age_Bucket")
      .agg(
        count("*").as("Total_Transactions"),
        sum(when(col("Is_Fraudulent") === 1, 1)
          .otherwise(0)).as("Fraudulent_Transactions")
      )
      .withColumn("Fraudulent_Transaction_Ratio",
        concat(format_number((col("Fraudulent_Transactions") / col("Total_Transactions")) * 100, 3), lit("%")))
      .orderBy("Account_Age_Bucket")

    println("=== Transaction Counts by Account Age Bucket ===")
//    transactionCountsByAge.show()
    transactionCountsByAge.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","AccountTime_Frand",prop)

    // 需求3：筛选出每个产品类别中，Is Fraudulent 为 1 的情况下 Transaction Amount 的前五个最大值
    val fraudulentTransactions = rdd.filter(row => row
      .getInt(row.fieldIndex("Is_Fraudulent")) == 1)

    val categoryAmountPairs = fraudulentTransactions.map(row =>
      (row.getString(row.fieldIndex("Product_Category")),
        row.getDouble(row.fieldIndex("Transaction_Amount")))
    )

    // 将每个产品类别的金额按降序排序并取前五个最大值
    val top5AmountsByCategory = categoryAmountPairs.groupByKey()
      .mapValues { amounts =>
      amounts.toList.sorted(Ordering[Double].reverse).take(5)
    }
    // 将结果转换为 DataFrame
    val top5AmountsSeq = top5AmountsByCategory.flatMap { case (category, amounts) =>
      amounts.map(amount => Row(category, amount))
    }.collect()

    val top5AmountsSchema = new StructType()
      .add("Product_Category", StringType, false)
      .add("Transaction_Amount", DoubleType, false)

    val top5AmountsDf = spark.createDataFrame(
      spark.sparkContext.parallelize(top5AmountsSeq),
      top5AmountsSchema
    )
    // 保留两位小数
    val formattedDf = top5AmountsDf.withColumn("Transaction_Amount", round(col("Transaction_Amount"), 2))
    println("=== Top 5 Fraudulent Transaction Amounts by Product Category ===")
    formattedDf.show()
    formattedDf.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","categories_Frad",prop)
    // 需求4：统计欺诈发生最频繁的时间段
   /* val fraudHourTransactions = hashedData.filter($"Is_Fraudulent" === 1)
      .withColumn("Time_Slot", when($"Transaction_Hour" >= 23 || $"Transaction_Hour" <= 5, "23-5")
        .when($"Transaction_Hour" >= 6 && $"Transaction_Hour" <= 10, "6-10")
        .when($"Transaction_Hour" >= 11 && $"Transaction_Hour" <= 17, "11-17")
        .otherwise("18-22"))

    val fraudHourCount = fraudHourTransactions.groupBy("Time_Slot").count()
    fraudHourCount.show()
    fraudHourCount.write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false", "Time_Range_Frad", prop)
    */

    // 需求5：根据Transaction Amount和Payment Method分析不同数额
    // Transaction amount范围下用户使用最频繁的支付方式
    val amountBuckets = when(col("Transaction_Amount") <= 100, "0-100")
      .when(col("Transaction_Amount") <= 500, "101-500")
      .when(col("Transaction_Amount") <= 1000, "501-1000")
      .otherwise("1000up")

    val dataWithAmountBucket = hashedData
      .withColumn("Amount_Range", amountBuckets)

    val mostFrequentPaymentMethodByAmount = dataWithAmountBucket
      .groupBy("Amount_Range", "Payment_Method")
      .agg(count("*").as("Count"))
      .withColumn("Rank", row_number()
        .over(Window.partitionBy("Amount_Range").orderBy(desc("Count"))))
      .filter($"Rank" === 1)
      .select("Amount_Range", "Payment_Method", "Count")

    val sortedFrequentPaymentMethodByAmount = mostFrequentPaymentMethodByAmount.withColumn("AmountRangeNum",
      when($"Amount_Range" === "0-100", 1)
        .when($"Amount_Range" === "101-500", 2)
        .when($"Amount_Range" === "501-1000", 3)
        .when($"Amount_Range" === "1000up", 4))
      .orderBy("AmountRangeNum")
      .drop("AmountRangeNum")

    println("==Transaction Amountpaymethod==")
    sortedFrequentPaymentMethodByAmount.na.drop().write.jdbc("jdbc:mysql://192.168.56.104:3306/sparkproject?useSSL=false","paymethodPrefer",prop)
    // 关闭Spark会话
    spark.stop()
  }
}