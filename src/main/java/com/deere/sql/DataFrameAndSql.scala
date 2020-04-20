package com.deere.sql

import org.apache.spark.sql.SparkSession

object DataFrameAndSql extends App {
  
  /*
   * in prior Spark2.x, SQlContext, HiveContext, SparkContext and SaprkStreaming
   * Where after Spark2.x we have only SparkSession
   */
  val spark:SparkSession = SparkSession.builder().appName("DataFrameAndSql").master("local[*]").getOrCreate()
  
  /*
   * DataFrame is a distributed collection of Structure Data in table format with schema
   */
  val df = spark.read.option("header", true).option("inferSchema", true).format("csv").load("C:/SparkScala/Data/Movie Ratings.csv")
  
  df.printSchema()
  df.show()
  
 
  df.select("UserId", "MovieId","UserId", "Timestamp").where("UserId > 10").show()
  
  
  df.createTempView("MovieRating")
  
  val sql = df.sqlContext.sql("SELECT UserId, MovieId, Rating, Timestamp FROM MovieRating where Rating >= 10 ")
  sql.show()
  
  
  
}