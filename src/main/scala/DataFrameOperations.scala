import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import s3Service.model.S3Request

object DataFrameOperations {

  val s3_transactions_path: String = "C:\\Users\\Bluetab\\IdeaProjects\\GraphFramesSparkPlayground\\" +
    "src\\resources\\aws\\s3_transactions\\*.csv"

  def inefficient_run(session: SparkSession) : Unit = {
    import session.sqlContext.implicits._
    val schema = Encoders.product[S3Request].schema
    session
      .read
      .option("header", "true")
      .schema(schema)
      .csv(s3_transactions_path)
      .groupBy("userID", "fromService")
      .count().as("count")
      .sort(desc("count"))
      .filter(row => row.getAs("fromService").equals("Lambda"))
      .show(10)
  }

  /**
   * Intended for measuring the time elapsed for reading an parsing
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
    val schema = Encoders.product[S3Request].schema
    session
      .read
      .option("header", "true")
      .schema(schema)
      .csv(s3_transactions_path)
      .filter(row => row.getAs("fromService").equals("Lambda"))
      .groupBy("userID", "fromService")
      .count().as("count")
      .sort(desc("count"))
      .show(10)
  }

}
