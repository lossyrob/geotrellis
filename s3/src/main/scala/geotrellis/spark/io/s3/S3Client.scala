package geotrellis.spark.io.s3

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.{AmazonS3Client => AWSAmazonS3Client}
import java.io.{InputStream, ByteArrayInputStream}
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._
import com.typesafe.scalalogging.LazyLogging
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import com.amazonaws.ClientConfiguration
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._

trait S3Client extends LazyLogging {

  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing

  def listObjects(bucketName: String, prefix: String): ObjectListing =
    listObjects(new ListObjectsRequest(bucketName, prefix, null, null, null))

  def listKeys(bucketName: String, prefix: String): Seq[String] =
    listKeys(new ListObjectsRequest(bucketName, prefix, null, null, null))

  def listKeys(listObjectsRequest: ListObjectsRequest): Seq[String]

  def getObject(getObjectRequest: GetObjectRequest): S3Object

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult

  def listNextBatchOfObjects(listing: ObjectListing): ObjectListing

  @tailrec
  final def deleteListing(bucket: String, listing: ObjectListing): Unit = {
    deleteObjects(bucket, listing.getObjectSummaries.map { os => new KeyVersion(os.getKey) }.toList)
    if (listing.isTruncated) deleteListing(bucket, listNextBatchOfObjects(listing))
  }

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Unit

  def copyObject(copyObjectRequest: CopyObjectRequest): CopyObjectResult

  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Unit

  def getObject(bucketName: String, key: String): S3Object =
    getObject(new GetObjectRequest(bucketName, key))

  def deleteObjects(bucketName: String, keys: List[KeyVersion]): Unit = {
    val objectsDeleteRequest = new DeleteObjectsRequest(bucketName)
    objectsDeleteRequest.setKeys(keys.asJava)
    deleteObjects(objectsDeleteRequest)
  }

  def copyObject(sourceBucketName: String, sourceKey: String,
                 destinationBucketName: String, destinationKey: String): CopyObjectResult =
    copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey))

  def deleteObject(bucketName: String, key: String): Unit =
    deleteObject(new DeleteObjectRequest(bucketName, key))

  def putObject(bucketName: String, key: String, input: InputStream, metadata: ObjectMetadata): PutObjectResult =
    putObject(new PutObjectRequest(bucketName, key, input, metadata))

  def putObject(bucketName: String, key: String, bytes: Array[Byte], metadata: ObjectMetadata): PutObjectResult = {
    metadata.setContentLength(bytes.length)
    putObject(bucketName, key, new ByteArrayInputStream(bytes), metadata)
  }

  def putObject(bucketName: String, key: String, bytes: Array[Byte]): PutObjectResult =
    putObject(bucketName, key, bytes, new ObjectMetadata())

  def readBytes(bucketName: String, key: String): Array[Byte] =
    readBytes(new GetObjectRequest(bucketName, key))

  def readBytes(getObjectRequest: GetObjectRequest): Array[Byte]
  
  def readRange(start: Long, end: Long, getObjectRequest: GetObjectRequest): Array[Byte]

  def getObjectMetadata(bucketName: String, key: String): ObjectMetadata =
    getObjectMetadata(new GetObjectMetadataRequest(bucketName, key))

  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): ObjectMetadata

  def listObjectsIterator(bucketName: String, prefix: String, maxKeys: Int = 0): Iterator[S3ObjectSummary] =
      listObjectsIterator(new ListObjectsRequest(bucketName, prefix, null, null, if (maxKeys == 0) null else maxKeys))

  def listObjectsIterator(request: ListObjectsRequest): Iterator[S3ObjectSummary] =
    new Iterator[S3ObjectSummary] {
      var listing = listObjects(request)
      var iter = listing.getObjectSummaries.asScala.iterator

      def getNextPage: Boolean =  {
        listing.isTruncated && {
          val nextRequest = request.withMarker(listing.getNextMarker)
          listing = listObjects(nextRequest)
          iter = listing.getObjectSummaries.asScala.iterator
          iter.hasNext
        }
      }

      def hasNext: Boolean = {
        iter.hasNext || getNextPage
      }

      def next: S3ObjectSummary = iter.next
    }

  def setRegion(region: com.amazonaws.regions.Region): Unit
}

object S3Client {
  def defaultConfiguration = {
    val config = new com.amazonaws.ClientConfiguration
    config.setMaxConnections(128)
    config.setMaxErrorRetry(16)
    config.setConnectionTimeout(100000)
    config.setSocketTimeout(100000)
    config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
    config
  }

  def default =
    new AmazonS3Client(new DefaultAWSCredentialsProviderChain(), defaultConfiguration)
}

class AmazonS3Client(s3client: AWSAmazonS3Client) extends S3Client {

  def this(credentials: AWSCredentials, config: ClientConfiguration) =
    this(new AWSAmazonS3Client(credentials, config))

  def this(provider: AWSCredentialsProvider, config: ClientConfiguration) =
    this(new AWSAmazonS3Client(provider, config))

  def this(provider: AWSCredentialsProvider) =
    this(provider, new ClientConfiguration())

  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing =
    s3client.listObjects(listObjectsRequest)

  def listKeys(listObjectsRequest: ListObjectsRequest): Seq[String] = {
    var listing: ObjectListing = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3client.listObjects(listObjectsRequest)
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")
      listObjectsRequest.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    result.toSeq
  }

  def getObject(getObjectRequest: GetObjectRequest): S3Object =
    s3client.getObject(getObjectRequest)

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult =
    s3client.putObject(putObjectRequest)

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Unit =
    s3client.deleteObject(deleteObjectRequest)

  def copyObject(copyObjectRequest: CopyObjectRequest): CopyObjectResult =
    s3client.copyObject(copyObjectRequest)

  def listNextBatchOfObjects(listing: ObjectListing): ObjectListing =
    s3client.listNextBatchOfObjects(listing)
  
  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Unit =
    s3client.deleteObjects(deleteObjectsRequest)

  def readBytes(getObjectRequest: GetObjectRequest): Array[Byte] = {
    val obj = s3client.getObject(getObjectRequest)
    val inStream = obj.getObjectContent
    try {
      IOUtils.toByteArray(inStream)
    } finally {
      inStream.close()
    }
  }

  def readRange(start: Long, end: Long, getObjectRequest: GetObjectRequest): Array[Byte] = {
    getObjectRequest.setRange(start, end - 1)
    val obj = s3client.getObject(getObjectRequest)
    val stream = obj.getObjectContent
    try {
      IOUtils.toByteArray(stream)
    } finally {
      stream.close()
    }
  }
  
  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): ObjectMetadata =
    s3client.getObjectMetadata(getObjectMetadataRequest)

  def setRegion(region: com.amazonaws.regions.Region): Unit = {
    s3client.setRegion(region)
  }
}
