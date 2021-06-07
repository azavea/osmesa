package osmesa.analytics

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets

import geotrellis.store.s3.S3ClientProducer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

object S3Utils {
  def readText(uri: String): String = {
    val s3Client: S3Client = S3ClientProducer.get()
    val s3uri = URI.create(uri)
    val key = URLDecoder.decode(s3uri.getPath.drop(1), StandardCharsets.UTF_8.toString)
    val request = GetObjectRequest.builder()
      .bucket(s3uri.getHost)
      .key(key)
      .build()
    s3Client.getObjectAsBytes(request).asString(StandardCharsets.UTF_8)
  }
}
