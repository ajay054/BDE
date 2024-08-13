package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.col

object DSDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("DataframeDemo")
    .set("spark.master", "local[*]")

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

  // explicit schema way2 – ddl string
  val ddlSchema =
    """
product_number string, product_name string, product_category string, product_scale string,
product_manufacturer string, product_description string, length double, width double, height float
""".stripMargin

  val productdf = spark.read.option("header", true)
    .schema(productschema)
    .csv("C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\clean\\productscleaned2")

  case class productdata(product_number: String
                         , product_name: String
                         , product_category: String
                         , product_scale: String
                         , product_manufacturer: String
                         , product_description: String
                         , length: Double
                         , width: Double
                         , height: Float
                        )

  import spark.implicits._

  val productds = productdf.as[productdata]

  // Correcting the typo from "widt" to "width"
  productds.filter(col("width") > 85).select("product_number", "width").show(5)

}
