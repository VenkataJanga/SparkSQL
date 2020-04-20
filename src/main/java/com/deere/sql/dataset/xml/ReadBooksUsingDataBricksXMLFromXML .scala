package com.deere.sql.dataset.xml

import org.apache.spark.sql.SparkSession
import com.deere.sql.beans.Books
import com.deere.sql.beans.BooksDiscounted

object ReadBooksUsingDataBricksXMLFromXML {
  
  def main(args: Array[String]): Unit = {
    
    val spark:SparkSession = SparkSession.builder().appName("ReadBooksFromXML").master("local[1]").getOrCreate()
    import spark.implicits._
    
    val ds = spark.sqlContext.read.option("rowTag", "book")
                                  .format("com.databricks.spark.xml")
                                  .load("src/main/resources/Books.xml").as[Books]
    val dataset = ds.map(f=>{
       BooksDiscounted(f._id,f.author,f.description,f.price,f.publish_date,f.title, f.price - f.price*20/100)
      })
    dataset.printSchema()
    dataset.show()
    
    dataset.foreach(f=>{
        println("Price :"+f.price + ", Discounted Price :"+f.discountPrice)
      })
  
      //First element
      println("First Element" +dataset.first()._id)
  
  }
}