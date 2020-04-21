import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import s3Service.model.S3Request

object DataFrameOperations {

  val s3_transactions_path: String = "C:\\Users\\Bluetab\\IdeaProjects\\GraphFramesSparkPlayground\\" +
    "src\\resources\\aws\\s3_transactions\\*.csv"
  val lambda_usage_path: String = "C:\\Users\\Bluetab\\IdeaProjects\\GraphFramesSparkPlayground\\" +
    "src\\resources\\aws\\lambda_usage\\*.csv"

  def inefficient_run(session: SparkSession) : Unit = {
    import session.sqlContext.implicits._
    val lambdaUsageDF: DataFrame = session
      .read
      .option("header", "true")
      .csv(lambda_usage_path)
      .withColumnRenamed("userID", "lambdaUserID")
    session
      .read
      .option("header", "true")
      .csv(s3_transactions_path)
      .groupBy("userID", "fromService")
      .agg(
        count("uuid").as("numberTransactions")
      )
      .join(lambdaUsageDF, $"userID" === $"lambdaUserID")
      .groupBy("userID", "lambdaUserID", "fromService")
      .agg(
        sum("execution_time_ms").as("total_exec_time"),
        sum("billed").as("total_cost"),
        max("numberTransactions").as("numberTransactions")
      )
      .filter(row => row.getAs("fromService").equals("Lambda"))
      .sort(desc("numberTransactions"))
      .show(1)
  }

  /**
   * Intended for measuring the time elapsed for reading an parsing
   * Call an action, eg. show
   * @param session
   */
  def read(session: SparkSession): Unit = {
    val schema = Encoders.product[S3Request].schema
    session
      .read
      .option("header", "true")
      .schema(schema)
      .csv(s3_transactions_path)
  }

  def efficient_run(session: SparkSession) : Unit = {
    val lambdaUsageDF: DataFrame = session
        .read
        .option("header", "true")
        .csv(lambda_usage_path)
        .groupBy("userID")
        .agg(
          sum("billed").as("total_cost"),
          sum("execution_time_ms").as("total_exec_time")
        )
    session
      .read
      .option("header", "true")
      .csv(s3_transactions_path)
      .filter(row => row.getAs("fromService").equals("Lambda"))
      .groupBy("userID", "fromService")
      .agg(
        count("uuid").as("numberTransactions")
      )
      .join(lambdaUsageDF, "userID")
      .sort(desc("numberTransactions"))
      .show(1)
  }

}
