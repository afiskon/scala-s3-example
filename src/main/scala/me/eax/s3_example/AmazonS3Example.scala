package me.eax.s3_example

import com.amazonaws._
import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import java.io._

object AmazonS3Example extends App {
  val accessKey = "AKIAJNSEFYYHD43JLEKQ"
  val secretKey = "8c0NOloloTrololoOloloTrololoOloloTrololo"
  val bucketName = "eaxme-test"
  val urlPrefix = "https://s3-us-west-1.amazonaws.com"

  val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val client = new AmazonS3Client(credentials)

  def uploadToS3(fileName: String, uploadPath: String): String = {
    client.putObject(bucketName, uploadPath, new File(fileName))

    val acl = client.getObjectAcl(bucketName, uploadPath)
    acl.grantPermission(GroupGrantee.AllUsers, Permission.Read)
    client.setObjectAcl(bucketName, uploadPath, acl)

    s"$urlPrefix/$bucketName/$uploadPath"
  }

  def fileIsUploadedToS3(uploadPath: String): Boolean = {
    try {
      client.getObjectMetadata(bucketName, uploadPath)
      true
    } catch {
      case e: AmazonServiceException if e.getStatusCode == 404 =>
        false
    }
  }

  def downloadFromS3(uploadPath: String, downloadPath: String) {
    if(!fileIsUploadedToS3(uploadPath)) {
      throw new RuntimeException(s"File $uploadPath is not uploaded to S3")
    }
    client.getObject(new GetObjectRequest(bucketName, uploadPath), new File(downloadPath))
  }

  if(args.length < 2) {
    println("Usage: prog.jar file.dat s3/upload/path.dat local/download/path.dat")
  } else {
    val Array(fileName, uploadPath, downloadPath, _*) = args
    println(s"Uploading $fileName...")

    val url = uploadToS3(fileName, uploadPath)
    println(s"Uploaded: $url")

    downloadFromS3(uploadPath, downloadPath)
    println(s"Downloaded: $downloadPath")
  }
}
