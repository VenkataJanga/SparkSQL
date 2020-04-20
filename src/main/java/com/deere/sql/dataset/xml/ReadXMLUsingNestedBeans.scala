package com.deere.sql.dataset.xml

import org.apache.spark.sql.SparkSession
import com.deere.sql.beans.BooksWithArray

object ReadXMLUsingNestedBeans extends App {
   val sparkSession = SparkSession.builder().appName("ReadXMLUsingNestedBeans").master("local").getOrCreate()
   import sparkSession.implicits._
   
   val dataset = sparkSession.sqlContext.read
                                        .format("com.databricks.spark.xml")
                                        .option("rowTag", "book")
                                        .load("src/main/resources/books_withnested_array.xml").as[BooksWithArray]
  dataset.printSchema()
  dataset.show()
  println(dataset.count())
  
  
  dataset.foreach(f =>{
    println(f.author+","+f.otherInfo.country+","+f.otherInfo.address.addressline1)
    
    for(s <- f.stores.store){
       println(s.name)
    }
    
    
  })
}