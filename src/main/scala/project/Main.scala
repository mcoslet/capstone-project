package project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import project.util.ParseArgs

object Main {
  def main(args: Array[String]): Unit = {
    val parseArgs = new ParseArgs(args)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("Capstone Project").getOrCreate()
    val mobile_df = readFromSource(sparkSession, parseArgs.mobilePath.getOrElse(""))
    val purchase_df = readFromSource(sparkSession, parseArgs.userPath.getOrElse(""))

    val mobileWithAttributes = sessionization(mobile_df, sparkSession)
    val purchasesAttributionProjection = mobileWithAttributes
      .join(purchase_df.select("purchaseId", "purchaseTime", "billingCost", "isConfirmed"), usingColumn = "purchaseId")
    purchasesAttributionProjection.createOrReplaceTempView("purchasesAttributionProjection")

    val topCampaignsSQL = sparkSession.sql(
      "SELECT campaignId, SUM(billingCost)" +
        "as acct_order_rank " +
        "FROM purchasesAttributionProjection WHERE isConfirmed = true AND campaignId IS NOT NULL GROUP BY campaignId ORDER BY 2 DESC")

    val topCampaingsDF = purchasesAttributionProjection.select(col("campaignId"),col("billingCost").cast("Double"),col("isConfirmed"))
          .where(col("isConfirmed") === "true").where(col("campaignId").isNotNull).groupBy("campaignId").sum("billingCost").orderBy(desc("sum(billingCost)"))


    topCampaingsDF.orderBy().show(10, truncate = false)
    topCampaignsSQL.show(10, truncate = false)

    val mostPopularChannelSQL = sparkSession.sql("select * from (SELECT ROW_NUMBER() OVER(PARTITION BY l.campaignId  " +
      "ORDER BY  l.countBillingCost desc) as rowNumber" +
      ", l.campaignId, l.channelId, l.countBillingCost " +
      "FROM (SELECT s.campaignId, s.channelId, COUNT(s.channelId) as countBillingCost FROM purchasesAttributionProjection s" +
      " GROUP BY s.campaignId, s.channelId) as l ) " +
      "where rowNumber = 1 and campaignId IS NOT NULL")

    mostPopularChannelSQL.show(false)
  }

  /**
   * Method used to read data from csv
   * @param sparkSession used to read data
   * @param path where to read the data
   * @return dataframe containing data from csv
   */
  def readFromSource(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read.options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv(path)
  }

  /**
   * Generate a unique uuid using java.util
   * @return a unique uuid
   */
  def generateUUID: UserDefinedFunction = udf((str: String) => java.util.UUID.nameUUIDFromBytes(str.getBytes()).toString)

  /**
   * This method takes a dataframe containing attributes in json format, and converts each item from json to a column
   * @param df dataframe containing attributes in json format
   * @return dataframe with new columns(campaign_id, channel_id, purchase_id)
   */
  def getFromAttributes(df: DataFrame): DataFrame = {
    val dfWithAttributes = df.withColumn("value", from_json(
      regexp_replace(col("attributes"), "'", "\""),
      lit("map<string,string>")
    ))
    dfWithAttributes
      .withColumn("campaign_id", col("value.campaign_id"))
      .withColumn("channel_id", col("value.channel_id"))
      .withColumn("purchaseId", col("value.purchase_id"))
  }

  /**
   * This method produces a unique ID using the window function
   * @param df dataframe what have to get a unique id
   * @param sparkSession used for implicits
   * @return
   */
  def sessionization(df: DataFrame, sparkSession: SparkSession):DataFrame = {
    import sparkSession.implicits._
    val partitionedByUserIdAndOrdered = Window.partitionBy($"userId").orderBy($"eventTime")
    val dfWithAttributes = getFromAttributes(df)
    dfWithAttributes.withColumn("isNewSession",
      when(col("eventType") === "app_open", 1).otherwise(0)
    )
      .withColumn("isNewSession",
        when($"eventType" === "app_open", 1).otherwise(0)
      )
      .withColumn(
        "sessionId",
        generateUUID(sum($"isNewSession").over(partitionedByUserIdAndOrdered))
      )
      .withColumn(
        "channelId",
        when($"sessionId" ===
          lag($"sessionId", 1).over(partitionedByUserIdAndOrdered),
          last($"channel_id", ignoreNulls = true).over(partitionedByUserIdAndOrdered))
          .otherwise($"channel_id")
      )
      .withColumn(
        "campaignId",
        when($"sessionId" ===
          lag($"sessionId", 1).over(partitionedByUserIdAndOrdered),
          last($"campaign_id", ignoreNulls = true).over(partitionedByUserIdAndOrdered))
          .otherwise($"campaign_id")
      )
      .select(col("sessionId"), col("campaignId"), col("channelId"), col("purchaseId"))
  }
}
