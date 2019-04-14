package example

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

case class Ratings(movieID: Integer,title: String, genres:String) //Dataset format

object test {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").
    appName("loadFromHdfs").getOrCreate()
    val movieDF= spark.read.csv("hdfs://192.168.18.98:9000/movies.csv")
    // load data from HDFS through DataFrame 
    
    
    val mSchema= movieDF.schema 
    println(mSchema)
    val mNames= movieDF.dtypes
    mNames.foreach(println)
    movieDF.head(10).foreach(println)
    movieDF.show(10)
   import spark.implicits._
    
    
    val ratingsDS= spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://192.168.18.98:9000/movies.csv")
    .as[Ratings]
    
    ratingsDS.show()
//    create dataset format data 
    
    
    //load data from HDFS TO MYSQL
    val url ="jdbc:mysql://192.168.18.99:3306/spark?serverTimezone=UTC"
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","123")
    Class.forName("com.mysql.jdbc.Driver")
    
    
    val movie= spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://192.168.18.98:9000/movies.csv")
   
    val table = "movie"
    movie.write.mode(SaveMode.Overwrite).jdbc(url,table,properties)
    
    
  }
}