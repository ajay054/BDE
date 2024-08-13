package org.itc.com
package DFDemo_transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, col, max, min, rank, split, when}

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
    .csv("C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\clean\\productscleaned2")

  // Select specific columns including 'product_category', filter, and order the data
  var selecteddf = productdf.select("product_number", "product_description", "length", "product_category", "product_name", "width")
    .filter(col("length") > 100)
    .orderBy("product_description")

  // Add a new column "product_size" based on conditions for "length"
  selecteddf = selecteddf.withColumn("product_size",
    when(col("length") < 4000, "Small")
      .when(col("length") < 6000, "Medium")
      .when(col("length") < 8000, "Large")
      .otherwise("Extra Large")
  )

  // Define a window specification and rank the data
  val windowspec = Window.partitionBy("product_category").orderBy(col("length").desc)
  val rankeddf = selecteddf.withColumn("Ranking", rank().over(windowspec))

  // Find outliers for length and width
  def findOutliers(df: org.apache.spark.sql.DataFrame, colName: String): org.apache.spark.sql.DataFrame = {
    val quantiles = df.stat.approxQuantile(colName, Array(0.25, 0.75), 0.0)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1
    val lowerBound = q1 - 1.5 * iqr
    val upperBound = q3 + 1.5 * iqr

    df.filter(col(colName) < lowerBound || col(colName) > upperBound)
  }

  val lengthOutliers = findOutliers(rankeddf, "length")
  val widthOutliers = findOutliers(rankeddf, "width")

  println("Length Outliers:")
  lengthOutliers.show()

  println("Width Outliers:")
  widthOutliers.show()

  // Add store_name and product_number_split columns
  val dfWithStoreAndProduct = rankeddf.withColumn("store_name", split(col("product_number"), "_").getItem(0))
    .withColumn("product_number_split", split(col("product_number"), "_").getItem(1))

  // Add product_classification column based on length and width
  val dfWithClassification = dfWithStoreAndProduct.withColumn("product_classification",
    when(col("length") > 8000 && col("width") > 60, "large and wide")
      .when(col("length") > 6000 && col("width") > 40, "small and narrow")
      .when(col("length") > 4000 && col("width") > 20, "small and wide")
      .otherwise("large and narrow")
  )

  // Find min, max, avg of length for each category
  val lengthStats = dfWithClassification.groupBy("product_category")
    .agg(
      min("length").as("min_length"),
      max("length").as("max_length"),
      avg("length").as("avg_length")
    )

  // Show the final DataFrame
  dfWithClassification.show()
  lengthStats.show()

  // Additional pivot operation if needed
  val resdf = rankeddf
    .groupBy("product_name")
    .pivot("product_category")
    .agg(functions.min(col("length")))

  resdf.show()
}
