import s3Service.model.{LambdaUsage, S3Request}

object Parsers {

  def s3Request(row: String): S3Request = {
    if (row.contains("epoch") || row.isEmpty) { // naive condition for checking whether it is a header or empty row
      null
    } else {
      val splitRow: Array[String] = row.split(",")
      S3Request(splitRow(0), splitRow(1).split("\\.")(0).toLong,
        splitRow(2), splitRow(3).toLong, splitRow(4), splitRow(5))
    }
  }

  def lambdaUsage(row: String): LambdaUsage = {
    if (row.contains("userID") || row.isEmpty) { // naive condition for checking whether it is a header or empty row
      null
    } else {
      val splitRow: Array[String] = row.split(",")
      LambdaUsage(splitRow(0).toInt, splitRow(1).toInt, splitRow(2).toFloat)
    }
  }

}
