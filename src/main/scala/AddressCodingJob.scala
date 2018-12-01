import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AddressCodingJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("AddressCoding")
      .getOrCreate()

    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("address_data.csv")

    val grouper = new HouseholdGrouper(spark)
    val groups: DataFrame = grouper.compute(df)
        saveToCsv(groups)
    val durationColumn = col("End_date").cast("Int") minus col("start").cast("Int")
    val durationMean = groups.select(mean(durationColumn))
    val numberOfGroups = groups.count()
    print("Total number of groups: " + numberOfGroups)
    print("Average duration for group: " + durationMean.first())
  }

  private def saveToCsv(groups: DataFrame) = {
    groups
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("groups.csv")
  }
}
