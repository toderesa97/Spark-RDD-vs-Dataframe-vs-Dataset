package s3Service.model

case class S3Request(uuid: String, epoch: Long, requestType: String, userID: Long, fromService: String, resource: String)