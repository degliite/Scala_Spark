package com.github.degliite.stocks

import com.github.degliite.stocks.StockPrice.df
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr}

object StockPricesAnalysis extends App{

  val session = SparkSession.builder().appName("StockPrices").master("local").getOrCreate()
  val fPath = "./src/main/resources/stock_prices.csv"

  val df = session.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(fPath)

  val dailyReturn = df
    .groupBy(col("date"))
    .agg(avg("close").alias("average_return"))
    .withColumn("average_return_rounded", expr("ROUND(average_return, 2)"))
    .orderBy(col("date"))

//  //lets make a column indicated total spent so Quantity * UnitPrice
//  val df2 = df.withColumn("Total", expr("ROUND(Quantity * UnitPrice, 2)"))
//  df2.show(10)
//  val topPurchases = df2.sort(desc("Total"))
//  topPurchases.show(10)
//  val returns = topPurchases.sort("Total").where(expr("Total < 0"))
//  println(returns.count)

  val topStocks = df.withColumn("Top_Stocks", expr("ROUND("))
  val mostFrequentStock =session.sql("SELECT ticker, ROUND((SUM(close * volume)/COUNT(volume))/1000,2) AS frequency_thousands " +
    "FROM stock_prices_view " +
    "GROUP BY ticker " +
    "ORDER BY frequency_thousands DESC " +
    "LIMIT 1")

  val mostFrequentStockName = mostFrequentStock.select(col("ticker")).first().toString().stripPrefix("[").stripSuffix("]")
  val mostFrequentStockFrequency = mostFrequentStock.select(col("frequency_thousands")).first().toString().stripPrefix("[").stripSuffix("]")

  println(s"The stock that was traded most frequently on average was $mostFrequentStockName with frequency of $mostFrequentStockFrequency thousands.")
}
