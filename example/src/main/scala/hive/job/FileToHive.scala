package hive.job

import org.apache.spark.sql.SparkSession

object FileToHive extends App {
  val spark = SparkSession.builder.master("local").enableHiveSupport(). //connect to hive
    appName("loadFromHdfs").getOrCreate()
    
  val movieDF= spark.read.option("header","true")
  .csv("hdfs://192.168.18.98:9000/movies.csv")
  
    
    //from HDFS to hive table , format:DBNAME.TABLENAME 
    movieDF.write.saveAsTable("spark_test.mov4ie")
}