import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import s3Service.model.{GroupedS3Requests, S3Request, UserService}
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.internal.config.ConfigBuilder

import scala.math.pow

object Main {


  def getExcludeRules: Seq[String] = {
    Seq(
      PushPredicateThroughJoin.ruleName
    )
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println("\n----------------------------------------------------------------------------\n")
    var c = System.nanoTime()
    //DataFrameOperations.efficient_run(spark)
    println("DF,efficient," + (System.nanoTime() - c)/pow(10, 9))
    println("\n----------------------------------------------------------------------------\n")
    c = System.nanoTime()
    //RDDOperations.efficient_run(sc)
    println("RDD,efficient," + (System.nanoTime() - c)/pow(10, 9))
    println("\n----------------------------------------------------------------------------\n")
    c = System.nanoTime()
    //RDDOperations.inefficient_run(sc)
    println("RDD,inefficient," + (System.nanoTime() - c)/pow(10, 9))
    println("\n----------------------------------------------------------------------------\n")
    c = System.nanoTime()
    DataFrameOperations.inefficient_run(spark)
    println("DF,inefficient," + (System.nanoTime() - c)/pow(10, 9))


  }



}
