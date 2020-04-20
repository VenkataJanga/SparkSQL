package com.deere.sql

import org.apache.spark.sql.SparkSession

import com.deere.sql.beans.Name


object NameEncoders {
  implicit def testEncoder: org.apache.spark.sql.Encoder[Name] =
    org.apache.spark.sql.Encoders.kryo[Name]
}

object CreateCustomDataSet extends App {
  
  val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("CreateCustomDataSet")
      .getOrCreate()
  import spark.sqlContext.implicits._   
  val name:Name = new Name("first_name","middle_name","last_name")
  
  import spark.sqlContext.implicits._
  import org.apache.spark.sql.Encoders
  import NameEncoders._
    
  
  val data = Seq(name)
  val rdd = spark.sparkContext.parallelize(data)
  val ds = spark.createDataset(data)
  ds.printSchema()
  
  val ds2 = ds.selectExpr("CAST(value AS String)").as[(String)]
  
  ds2.show(false)
  
  
}