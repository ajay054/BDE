package org.itc.com
package DFDemo_transformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, lower, rank, trim, when}

object DFDemo extends App {

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

  // Load the CSV file with the defined schema
  val productdf = spark.read.option("header", true)
    .schema(productschema)
    .csv("\"C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\clean\\")

  // Select specific columns, filter and order the data
  var selecteddf = productdf.select("product_number", "product_description", "length")
    .filter(col("length") > 100)
    .orderBy("product_description")

  // Add a new column "product_size" based on conditions for "length"
  selecteddf = selecteddf.withColumn("product_size",
    when(col("length") < 4000, "Small")
      .when(col("length") < 6000, "Medium")
      .when(col("length") < 8000, "Large")
      .otherwise("Extra Large")
  )

  val windowspec = Window.partitionBy( colName = "product_category").orderBy( col(colName= "length").desc)
  val rankeddf = selecteddf.withColumn(colName = "Ranking", rank().over(windowspec))
  rankeddf.show( numRows = 20)

  //pivot
//  val resdf = rankeddf.groupBy( col1 = "product_name" ).pivot( pivotColumn = "product_category").agg(functions.min(col(colName = "length"
//    resdf.show()
//  ))
//  selecteddf.printSchema()
//  selecteddf.show(numRows = 5)
}

