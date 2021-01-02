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

  //Creating a view for SQL
  df.createOrReplaceTempView("stocks_view")

  //Using SQL to get the top sold stocks
  val topStocks = session.sql("SELECT ticker, SUM(close * volume) AS top_stocks " +
    "FROM stocks_view " +
    "GROUP BY ticker " +
    "ORDER BY top_stocks ")


  // Showing the result of the most traded stock in 2 ways:
  println(s"The most traded stock was ${topStocks.select(col("ticker")).first()} " +
    s"with volume ${topStocks.select(col("top_stocks")).first()}.")

  println("The most traded stock was:")
  topStocks.show(1)


}
