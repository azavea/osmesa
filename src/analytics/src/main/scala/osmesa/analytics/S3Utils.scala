package osmesa.analytics

import com.amazonaws.services.s3.AmazonS3URI
import geotrellis.spark.io.s3.S3Client

import scala.io.Source
import java.nio.charset.Charset

object S3Utils {
  def readText(uri: String): String = {
    val s3Uri = new AmazonS3URI(uri)
    val is = S3Client.DEFAULT.getObject(s3Uri.getBucket, s3Uri.getKey).getObjectContent
    try {
      Source.fromInputStream(is)(Charset.forName("UTF-8")).mkString
    } finally {
      is.close()
    }
  }
}