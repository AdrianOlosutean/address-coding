import org.apache.spark.sql.{DataFrame, SparkSession}

class HouseholdGrouper(spark: SparkSession) {

  def compute(df: DataFrame): DataFrame = {
    import spark.implicits._

    val groups = df.as[AddressData]
      .groupByKey(_.addressId)
      .mapGroups(Merger.mergeFunctionally)
      .flatMap(x => x)
      .withColumnRenamed("address_id", "Address_ID")
      .withColumnRenamed("start ", "Start_date")
      .withColumnRenamed("end", "End_date")
    groups
  }
}
