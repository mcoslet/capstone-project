import org.scalatest._
import flatspec._
import matchers._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import project.Main.{getFromAttributes, sessionization}
import project.util.ParseArgs
class Main extends AnyFlatSpec with should.Matchers {
  val sparkSession:SparkSession = SparkSession.builder()
    .master("local[*]").appName("Testing")
    .getOrCreate()

  Logger.getLogger("org.apache").setLevel(Level.WARN)

  "SUM(billingCost)" should "find Top 10 marketing campaigns with mock data" in {
    val data = Seq(
      (8,97,false),
      (9,4,true),
      (8,89,false),
      (9,23,true),
      (10,69,false),
      (1,84,true),
      (5,57,false),
      (7,3,true),
      (6,14,false),
      (7,7,false),
      (9,44,true),
      (5,20,true),
      (8,48,true),
      (2,15,false),
      (2,22,false),
      (7,8,false),
      (7,28,true),
      (9,49,false),
      (9,43,false),
      (9,12,true),
      (2,9,true),
      (3,74,true),
      (3,13,true),
      (10,93,false),
      (6,25,false),
      (10,42,true)
    )
    val columns = Seq("campaignId","billingCost","isConfirmed")
    import sparkSession.implicits._
    val df = data.toDF(columns:_*)
    df.createOrReplaceTempView("purchasesAttributionProjection")
    val topCampaignsSQL = sparkSession.sql(
      "SELECT campaignId, SUM(billingCost)" +
        "as acct_order_rank " +
        "FROM purchasesAttributionProjection WHERE isConfirmed = true AND campaignId IS NOT NULL GROUP BY campaignId ORDER BY 2 DESC")

    val revenue = topCampaignsSQL.where("campaignId= 2").collect()(0).get(1).toString

    assert(revenue.equals("9"),
      "CampaignId 2 revenue = 9")
  }

  "getFromAttributes()" should "correctly get data from attributes" in {
    val data = sparkSession.read.option("header", value = true).csv("capstone-dataset/test_mobile/mobile_app_clickstream_0.csv.gz")
    getFromAttributes(data).where(col("userId") === "f6e8252f-b5cc-48a4-b348-29d89ee4fa9e").where(col("eventType") === "app_open").select("attributes", "value")
      .show(false)
  }

  "sessionization" should "correctly give unique sessionId" in {
    val data = sparkSession.read.option("header", value = true).csv("capstone-dataset/test_mobile/mobile_app_clickstream_0.csv.gz")
    //dropDuplicates is used because the session is unique, but for each session we have several campaign id that are the same
    val result = sessionization(data, sparkSession).dropDuplicates("sessionId")

    result.show()
    assert(result.count() == 1)
  }

  "ParseArgs" should "correctly parse args" in {
    val args = Array("--mobile-path", "capstone-dataset/mobile_app_clickstream", "--user-path", "capstone-dataset/user_purchases")
    val parseArgs = new ParseArgs(args)
    assert(parseArgs.userPath.getOrElse("").equals("capstone-dataset/user_purchases"))
  }
}
