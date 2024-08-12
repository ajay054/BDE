//package org.itc.com
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//object DFDemo extends App {
//
//  Logger.getLogger("org").setLevel(Level.ERROR)
//
//  val sparkConf = new SparkConf()
//    .setAppName("DataframeDemo")
//    .set("spark.master","local[*]")
//
//  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//  val productdf = spark.read.option("header", true).option("inferSchema", true).csv( path = "C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\products.csv")
//
//  productdf.printSchema()
//
//  productdf.show( numRows = 5)
//
//}
package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType, FloatType}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.functions.lower

object DFDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("DataframeDemo")
    .set("spark.master","local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val productschema = StructType(Array(
    StructField("product_number", StringType, nullable = false),
    StructField("product_name", StringType, nullable = false),
    StructField("product_category", StringType, nullable = false),
    StructField("product_scale", StringType, nullable = false),
    StructField("product_manufacturer", StringType, nullable = false),
    StructField("product_description", StringType, nullable = false),
    StructField("length", DoubleType, nullable = false),
    StructField("width", DoubleType, nullable = false),
    StructField("height", FloatType, nullable = false)
  ))
  // explicit schema way2 - DDL string
  val ddlschema =
    """
      |product_number string, product_name string, product_category string, product_scale string,
      |product_manufacturer string, product_description string, length double, width double, height float
  """.stripMargin

  val productdf = spark.read
    .schema(productschema) // Use the defined schema
    .option("header", true)
    .csv("C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\products.csv")
// chane datatype from double to float
 //val casteddf = productdf.withColumn(colName = "Length", col("Length").cast("Float"))
var casteddf = productdf.withColumn("Length", col("Length").cast("Float"))

  // Remove duplicates based on "product_number" column
  casteddf = casteddf.dropDuplicates("product_number")

  // Handling missing values
  //val cleaneddf = casteddf.na.fill("unknown", Seq("product_name"))
  //  .na.fill(0, Seq("length", "width", "height"))
  // Handling missing values
  var cleaneddf = casteddf.na.fill("unknown", Seq("product_name"))
    .na.fill(0, Seq("length", "width", "height"))

  // Trim spaces at the beginning and end of the "product_name" column
  cleaneddf = cleaneddf.withColumn("product_name", trim(col("product_name")))

  // Change case to lowercase for specific columns
  cleaneddf = cleaneddf
    .withColumn("product_category", lower(col("product_category")))
    .withColumn("product_name", lower(col("product_name")))

  // Filter valid data

  //drop colu



  casteddf.printSchema()

  casteddf.show(numRows = 5)

//  productdf.printSchema()
//
//  productdf.show(numRows = 5)
}
