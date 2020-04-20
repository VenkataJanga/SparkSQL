package com.deere.sql
import org.apache.spark.sql.SparkSession
object MentalHealthSurvey extends App{
  val spark:SparkSession  = SparkSession.builder()
                            .appName("MentalHealthSurvey")
                            .master("local").getOrCreate()
                            
  import spark.implicits._
  import spark.sqlContext.implicits._  
   import org.apache.spark.sql.functions.{when, _} 
  val path = "C:/SparkScala/Data/mental-health-in-tech-survey/"
  val df = spark.read.option("header", true)
                      .option("nullValue", "NA")
                      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm?:ss")
                      .option("mode", "failfast")
                      .option("inferSchema", true).format("csv").load(path+"survey.csv")
  df.printSchema()
  df.show(5)
  df.createTempView("surveys")
 
  
   val  df1 = df.withColumn("ALLYES",when(col("treatment") === "Yes",1).when(col("treatment") === "No",0)) 
    val df2 = df1.withColumn("ALLNO",when(col("Treatment") === "no",1).when(col("Treatment") === "yes",0)) 
   
 //Create a local function
  val parseGender = (s: String) => {
                           if (List("cis female", "f", "female", "woman", "femake", "female ",
                             "cis-female/femme", "female (cis)", "femail").contains(s.toLowerCase))
                            "Female"
                           else if (List("male", "m", "male-ish", "maile", "mal", "male (cis)",
                             "make", "male ", "man", "msle", "mail", "malr", "cis man", "cis male").contains(s.toLowerCase))
                            "Male"
                           else
                            "Transgender"
                        }

  //Register the function as UDF
  spark.udf.register("parsed_gender", parseGender)
  
 
  
  //Apply the UDF
  //spark.sql("select parsed_gender(gender) as parsed_gender, * from surveys").show()
  
 /* spark.sql("select parsed_gender(gender) as parsed_gender, age from surveys limit 10").show()
  
  spark.sql("select parsed_gender(gender) as parsed_gender, * from surveys")
  .write
  .mode("overwrite")
  .saveAsTable("transformed_survey")*/
  
  
  
 
  
  /*val df1 = df.select("Gender", "treatment")
  
  df2.show()*/
 // val df3 = df1.withColumn("ALLNO",when(col("Treatment") === "no",1).when(col("Treatment") === "yes",0)) 
 // df3.show()

  
}