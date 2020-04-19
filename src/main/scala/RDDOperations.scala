import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import s3Service.model.{GroupedS3Requests, LambdaUsage, S3Request}

object RDDOperations {

  val s3_transactions_path: String = "C:\\Users\\Bluetab\\IdeaProjects\\GraphFramesSparkPlayground\\" +
    "src\\resources\\aws\\s3_transactions\\*.csv"
  val lambda_usage_path: String = "C:\\Users\\Bluetab\\IdeaProjects\\GraphFramesSparkPlayground\\" +
    "src\\resources\\aws\\lambda_usage\\*.csv"

  def inefficient_run(sc: SparkContext): Unit = {
    val result = sc
      .textFile(s3_transactions_path)
      .map(Parsers.s3Request)
      .filter(obj => obj != null)
      .map{s3req => ((s3req.userID, s3req.fromService), 1)}
      .reduceByKey(_+_)
      .sortBy(_._2, ascending = false)
      .filter(_._1._2.equals("Lambda"))
      .take(10)

    println(result.mkString("\n"))
  }

  /**
   * Intended for measuring the time elapsed for reading an parsing
   * @param sc
   */
  def read(sc: SparkContext): Unit = {
    sc
      .textFile(s3_transactions_path)
      .map(Parsers.s3Request)
      .filter(obj => obj != null)
  }

  def efficient_run(sc: SparkContext): Unit = {
    val lambda_usage_rdd: RDD[(Long, (Int, Float))] = sc
      .textFile(lambda_usage_path)
      .map{row =>
        if (row.contains("userID") || row.isEmpty) {
          null
        } else {
          val split: Array[String] = row.split(",")
          (split(0).toLong, (split(1).toInt, split(2).toFloat))
        }
      }
      .filter(t => t != null)
      .reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      })
    val result = sc
      .textFile(s3_transactions_path)
      .map(Parsers.s3Request)
      .filter(s3req => s3req != null && s3req.fromService.equals("Lambda"))
      .map{s3req => ((s3req.userID, s3req.fromService), 1)}
      .reduceByKey(_+_)
      .map{tuple => (tuple._1._1, (tuple._2))}
      .join(lambda_usage_rdd)
      .sortBy(_._2._1, ascending = false)
      .take(10)

    println(result.mkString("\n"))
  }

}
