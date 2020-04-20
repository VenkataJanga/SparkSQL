package com.deere.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object CricketAnalysisUsingSparkSQL1 extends App{
   val spark:SparkSession = SparkSession.builder().appName("CricketAnalysisUsingSparkSQL").master("local").getOrCreate()
   val filePath = "C:/SparkScala/Data/"
   import org.apache.spark.sql.functions._
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
   dataFrame.createTempView("CricketBatsmenSql")
   spark.sql(" SELECT PlayerCountry, Average FROM CricketBatsmenSql limit 10").show()
                    
   val parseSql = spark.sql(" SELECT PlayerCountry, Average FROM CricketBatsmenSql where Average > 50 ")
                      .orderBy(desc("Average"))
  val result =  parseSql.show(5)
  println(result)
  
                   
  // val parseSql1 = spark.sql(" SELECT PlayerCountry, Average FROM CricketBatsmenSql here Average > 50 sort by Average desc limit 3 ")
                     
  //parseSql1.show(false)
                    
   spark.sql("""select rtrim(split(PlayerCountry, '#')[1]) as country
                        ,count(rtrim(split(PlayerCountry, '#')[1])) as count 
                        from CricketBatsmenSql 
                        where Average > 50
                        group by rtrim(split(PlayerCountry, '#')[1]) 
                        order by 2 desc limit 3
                        """).show(false)
}