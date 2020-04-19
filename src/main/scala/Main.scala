import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import s3Service.model.{GroupedS3Requests, S3Request, UserService}
import scala.math.pow

object Main {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var c = System.nanoTime()
    RDDOperations.read(sc)
    println("RDD,read," + (System.nanoTime() - c)/pow(10, 9))

    c = System.nanoTime()
    RDDOperations.inefficient_run(sc)
    println("RDD,inefficient," + (System.nanoTime() - c)/pow(10, 9))

    c = System.nanoTime()
    RDDOperations.efficient_run(sc)
    println("RDD,efficient," + (System.nanoTime() - c)/pow(10, 9))

    c = System.nanoTime()
    DataFrameOperations.read(spark)
    println("DF,read," + (System.nanoTime() - c)/pow(10, 9))

    c = System.nanoTime()
    DataFrameOperations.inefficient_run(spark)
    println("DF,inefficient," + (System.nanoTime() - c)/pow(10, 9))

    c = System.nanoTime()
    DataFrameOperations.efficient_run(spark)
    println("DF,efficient," + (System.nanoTime() - c)/pow(10, 9))

  }



}
