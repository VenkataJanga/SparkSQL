package com.deere.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType


object CricketAnalysisUsingSparkSQL extends App {
  val spark:SparkSession = SparkSession.builder().appName("CricketAnalysisUsingSparkSQL").master("local").getOrCreate()
  val filePath = "C:/SparkScala/Data/"
  val df = spark.read
                    .option("InferSchema",true)
                    .option("header", false)
                    .format("csv")
                    .load(filePath+"BattingAveragesTest.csv")
  df.printSchema()
  df.show(false)
  
   import spark.implicits._
   import org.apache.spark.sql.functions._
   
  val result = df.filter(" _c7 > 50").sort(desc("_c7")).select("_c0", "_c7").limit(3)
  result.show(false)
 
  val schema = StructType(Array(
                    StructField("PlayerCountry",StringType,true),
                    StructField("YearsActive",StringType,true),
                    StructField("TotalMatches",IntegerType,false),
                    StructField("Innings",IntegerType,false),
                    StructField("NotOuts",IntegerType,false),
                    StructField("TotalRuns",IntegerType,false),
                    StructField("HigestScore",IntegerType,false),
                    StructField("Average",DoubleType,false),
                    StructField("Hundreds",IntegerType,false),
                    StructField("Fifties",IntegerType,false),
                    StructField("DuckOuts",IntegerType,false)
                          ))
                          
   val dataFrame = spark.read
                    .option("InferSchema",true)
                    .option("header", false)
                    .format("csv")
                    .schema(schema)
                    .load(filePath+"BattingAveragesTest.csv")
                          
   // Creating table name
  dataFrame.createTempView("CricketBatsmenSql")
  
  spark.sql(" SELECT * FROM CricketBatsmenSql where Average >50 limit 3").orderBy(desc("Average")).show(false)
  
  val parseSql = spark.sql(" SELECT PlayerCountry, Average FROM CricketBatsmenSql where Average >50 limit 3")
                      .orderBy(desc("Average"))
  parseSql.show()
  
  
  val res = spark.sql("""select rtrim(split(PlayerCountry, '#')[1]) as country
                        ,count(rtrim(split(PlayerCountry, '#')[1])) as count 
                        from CricketBatsmenSql 
                        group by rtrim(split(PlayerCountry, '#')[1]) 
                        order by 2 desc
                        """)
 res.show()
 
}