import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {
  val APP_NAME = "iceberg-recommendation-pipeline"
  val ICEBERG_CATALOG = "my_catalog"
  val ICEBERG_RECOMMENDATIONS_TABLE =
    s"$ICEBERG_CATALOG.default.recommendations"
  val ICEBERG_INTERACTIONS_TABLE = s"$ICEBERG_CATALOG.default.interactions"
  val HDFS_PATH = "hdfs://namenode:9000/warehouse"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(APP_NAME)
      .master("local[*]")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        s"spark.sql.catalog.$ICEBERG_CATALOG",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(s"spark.sql.catalog.$ICEBERG_CATALOG.type", "hadoop")
      .config(s"spark.sql.catalog.$ICEBERG_CATALOG.warehouse", HDFS_PATH)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    setupIcebergTables(spark)

    val interactions = spark.table(ICEBERG_INTERACTIONS_TABLE)

    val transformed = transform(spark, interactions)

    val recommendations = getRecommendations(spark, transformed)

    recommendations
      .writeTo(ICEBERG_RECOMMENDATIONS_TABLE)
      .overwritePartitions()

    spark
      .sql(s"""
        SELECT snapshot_id, committed_at
        FROM $ICEBERG_RECOMMENDATIONS_TABLE.snapshots
        ORDER BY committed_at DESC
      """)
      .show(false)

    spark.stop()
  }

  def setupIcebergTables(spark: SparkSession): Unit = {
    if (!spark.catalog.tableExists(ICEBERG_INTERACTIONS_TABLE)) {
      spark.sql(s"""
        CREATE TABLE $ICEBERG_INTERACTIONS_TABLE (
          user_id STRING,
          item_id STRING,
          time_stamp LONG,
          rating DOUBLE,
          category STRING
        )
        USING iceberg
     """)
      generateRandomData(spark)
    }

    if (!spark.catalog.tableExists(ICEBERG_RECOMMENDATIONS_TABLE)) {
      spark.sql(s"""
        CREATE TABLE $ICEBERG_RECOMMENDATIONS_TABLE (
          user_id STRING,
          item_id STRING,
          latest_event_date DATE,
          avg_weighted_rating DOUBLE
        )
        USING iceberg
        PARTITIONED BY (latest_event_date)
      """)
    }
  }

  def generateRandomData(spark: SparkSession): Unit = {
    import spark.implicits._

    val categories = Seq("A", "B", "C", "D", "E")

    val fourteenDaysMs = 14 * 24 * 60 * 60 * 1000

    Seq
      .fill(1000)(
        (
          s"user${Random.nextInt(100)}",
          s"item${Random.nextInt(50)}",
          System.currentTimeMillis - Random.nextInt(fourteenDaysMs),
          1.0 + Random.nextInt(5),
          categories(Random.nextInt(5))
        )
      )
      .toDF("user_id", "item_id", "time_stamp", "rating", "category")
      .writeTo(ICEBERG_INTERACTIONS_TABLE)
      .append()
  }

  def transform(spark: SparkSession, df: DataFrame): DataFrame = {
    val timestampSecs = col("time_stamp") / 1000

    val categoryWeight = udf((cat: String) =>
      cat match {
        case "A" => 5.0
        case "B" => 4.0
        case "C" => 3.0
        case "D" => 2.0
        case _   => 1.0
      }
    )

    val sevenDaysMs = 7L * 24 * 60 * 60 * 1000

    val window = Window
      .partitionBy("user_id")
      .orderBy(col("time_stamp"))
      .rangeBetween(-sevenDaysMs, 0L)

    df.withColumn(
      "event_date",
      to_date(from_unixtime(timestampSecs, "yyyy-MM-dd"))
    ).withColumn("event_hour", hour(from_unixtime(timestampSecs)))
      .withColumn(
        "weighted_rating",
        col("rating") * categoryWeight(col("category"))
      )
      .withColumn("total_weighted_rating", sum("weighted_rating").over(window))
      .withColumn("interaction_count", count("*").over(window))
  }

  def getRecommendations(spark: SparkSession, df: DataFrame): DataFrame = {
    val window = Window
      .partitionBy("user_id")
      .orderBy(col("avg_weighted_rating").desc)

    df.groupBy("user_id", "item_id")
      .agg(
        max("event_date").as("latest_event_date"),
        avg("weighted_rating").as("avg_weighted_rating")
      )
      .withColumn("rank", row_number().over(window))
      .where(col("rank") <= 5)
      .drop("rank")
  }
}
