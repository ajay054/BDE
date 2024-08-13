package org.itc.com
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object DataFrames  {
  // Initializing Spark session
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Restaurant Analysis")
      .config("spark.master", "local")
      .getOrCreate()

    // Import implicits for $ notation and DataFrame functions like `show`
    import spark.implicits._

    // JDBC connection properties
    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "Aj@y_054")
    connectionProperties.put("driver", "org.postgresql.Driver")

    // Load all tables from PostgreSQL into DataFrames
    def loadTable(tableName: String): DataFrame = {
      spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    }

    // Load specific tables needed
    val customerDF = loadTable("customer")
    val salesDF = loadTable("sales")
    val menuDF = loadTable("menu")
    val membersDF = loadTable("members")

    // Questions 1-5: Using DataFrames

    // 1. Total amount each customer spent at the restaurant
    val totalSpentDF = salesDF.join(menuDF, "product_id")
      .groupBy("customer_id")
      .agg(sum("price").alias("total_spent"))
    totalSpentDF.show()

    // 2. How many days has each customer visited the restaurant?
    val visitDaysDF = salesDF
      .select("customer_id", "order_date")
      .distinct()
      .groupBy("customer_id")
      .agg(countDistinct("order_date").alias("visit_days"))
    visitDaysDF.show()

    // 3. The first item from the menu purchased by each customer
    val firstItemDF = salesDF.join(menuDF, "product_id")
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy("order_date")))
      .filter($"rank" === 1)
      .select("customer_id", "product_name")
    firstItemDF.show()

    // 4. The most purchased item on the menu and how many times it was purchased by all customers
    val mostPurchasedItemDF = salesDF.join(menuDF, "product_id")
      .groupBy("product_name")
      .agg(count("product_id").alias("purchase_count"))
      .orderBy(desc("purchase_count"))
      .limit(1)
    mostPurchasedItemDF.show()

    // 5. The most popular item for each customer
    val mostPopularItemDF = salesDF.join(menuDF, "product_id")
      .groupBy("customer_id", "product_name")
      .agg(count("product_id").alias("purchase_count"))
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(desc("purchase_count"))))
      .filter($"rank" === 1)
      .select("customer_id", "product_name")
    mostPopularItemDF.show()

    // Questions 6-10: Using Spark SQL

    // Register DataFrames as temporary views
    salesDF.createOrReplaceTempView("sales")
    menuDF.createOrReplaceTempView("menu")
    membersDF.createOrReplaceTempView("members")

    // 6. Which item was purchased first by the customer after they became a member?
    spark.sql("""
    SELECT s.customer_id, m.product_name, MIN(s.order_date) as first_purchase_date
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members ms ON s.customer_id = ms.customer_id
    WHERE s.order_date >= ms.join_date
    GROUP BY s.customer_id, m.product_name
    ORDER BY s.customer_id, first_purchase_date
    """).show()

    // 7. Which item was purchased just before the customer became a member?
    spark.sql("""
    WITH ranked_sales AS (
      SELECT s.customer_id, m.product_name, s.order_date,
      ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.order_date DESC) as rank
      FROM sales s
      JOIN menu m ON s.product_id = m.product_id
      JOIN members ms ON s.customer_id = ms.customer_id
      WHERE s.order_date < ms.join_date
    )
    SELECT customer_id, product_name, order_date
    FROM ranked_sales
    WHERE rank = 1
    """).show()

    // 8. Total items and amount spent for each member before they became a member
    spark.sql("""
    SELECT s.customer_id, COUNT(s.product_id) as total_items, SUM(m.price) as total_spent
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members ms ON s.customer_id = ms.customer_id
    WHERE s.order_date < ms.join_date
    GROUP BY s.customer_id
    """).show()

    // 9. Points calculation (including sushi 2x points multiplier)
    spark.sql("""
    SELECT s.customer_id,
      SUM(CASE
        WHEN m.product_name = 'sushi' THEN m.price * 20
        ELSE m.price * 10
      END) as total_points
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    GROUP BY s.customer_id
    """).show()

    // 10. Points calculation in the first week after joining (with 2x points on all items)
    spark.sql("""
    SELECT s.customer_id,
      SUM(CASE
        WHEN s.order_date BETWEEN ms.join_date AND ms.join_date + INTERVAL '7' DAY THEN m.price * 20
        WHEN m.product_name = 'sushi' THEN m.price * 20
        ELSE m.price * 10
      END) as total_points
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members ms ON s.customer_id = ms.customer_id
    WHERE s.order_date <= '2024-01-31'
    GROUP BY s.customer_id
    """).show()

    // Stop the Spark session
    spark.stop()
  }
}
