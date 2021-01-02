package com.github.degliite.stocks

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

  df.createOrReplaceTempView("stocks_view")
  val topStocks = session.sql("SELECT ticker, SUM(close * volume) AS top_stocks " +
    "FROM stocks_view " +
    "GROUP BY ticker " +
    "ORDER BY top_stocks "+
  "LIMIT 1")

  println(s"The most traded stock was ${topStocks.select(col("ticker")).first()} " +
    s"with volume ${topStocks.select(col("top_stocks")).first()}.")


//  //lets make a column indicated total spent so Quantity * UnitPrice
//  val df2 = df.withColumn("Total", expr("ROUND(Quantity * UnitPrice, 2)"))
//  df2.show(10)
//  val topPurchases = df2.sort(desc("Total"))
//  topPurchases.show(10)
//  val returns = topPurchases.sort("Total").where(expr("Total < 0"))
//  println(returns.count)

}
