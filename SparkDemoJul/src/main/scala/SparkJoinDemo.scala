package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}

object SparkJoinDemo  extends App {

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
    .csv("C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\products.csv")

  val orderitemschema = """
order_number string, product_number string, product_category string, price int, quantity int
""".stripMargin

  val oitemdf = spark.read.option("header", true)
    .schema(orderitemschema)
    .csv("C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\orderdetails.csv")

  val resdf = productdf.join(oitemdf, productdf.col("product_number") === oitemdf.col("product_number"))
 //val resdf = oitemf.join(broadcast(productdf),)
  // SQL Join
////  val sqlJoinDF = spark.sql(
//    """
//      |SELECT p.product_number, p.product_name, o.order_number, o.quantity, o.price
//      |FROM products p
//      |JOIN orderitems o
//      |ON p.product_number = o.product_number
//    """.stripMargin)
//
//  // Show the results
//  sqlJoinDF.show(numRows = 5)
resdf.show( numRows = 5)

}
