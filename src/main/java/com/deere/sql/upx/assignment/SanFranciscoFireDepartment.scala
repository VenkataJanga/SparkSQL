package com.deere.sql.upx.assignment

import org.apache.spark.sql.SparkSession

object SanFranciscoFireDepartment extends App{
  
    val spark:SparkSession = SparkSession.builder().appName("CricketAnalysisUsingSparkSQL").master("local").getOrCreate()
    //1.Load the data from file “fire.csv” into dataframe “input_df”
    val filePath = "C:/SparkScala/UPX/Spark SQL Assignment/fire.csv"
    
    val input_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(filePath)
    
    //2. Print the schema of the data
            input_df.printSchema()
    
    //3. In the Data Frame
        //a) Retrieve the first 5 records
            input_df.show(5)
    
        //b) Print just the column names
         println()
           input_df.columns
    
        //c) Count the total number of rows
            println(input_df.count)
      
   //4) How many different types of calls were made to the Fire Department?
          input_df.select("Call Type").distinct().show()
          val df1 = input_df.select("Call Type").distinct()
              println( df1.count())
              
      //5) How many incidents of each type were there
        input_df.select("Call Type").groupBy("Call Type").count().orderBy("count").show()
        
        
        //6) Count the number of partitions the data frame comprises of. If the number is greater than 6, reduce the number of partitions to 6.
          //input_df.rdd.getNumPartitions() 
          input_df.repartition(6)
          
          //7) Create an SQL table named “fire” from “input_df”
            input_df.createOrReplaceTempView("fire")
          //8) Use SQL queries to answer the following:
                //a) Print the schema of “fire” table
            spark.sql("DESC fire").show()
            
            //b) Count the number of rows in data
              spark.sql("SELECT count(*) FROM fire").show()
              
              //c) Show all the distinct city names in the table
              spark.sql("SELECT distinct City FROM fire where City !=null ").show()
              
              //d)Arrange the city names returned in b) in lexicographical order
              spark.sql("SELECT distinct City FROM fire ORDER BY City").show()
              
              //e) List all the distinct ‘Priorities’ in table
              spark.sql("SELECT distinct Priority FROM fire").show()
              
              
              //f) Which neighborhood generated the most number of calls
              spark.sql("SELECT City,count('Zipcode of IncidentIncident') AS Zip_Count FROM fire GROUP BY City ORDER BY 'Zip_Count' DESC").show()
              
            
}