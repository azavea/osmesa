package osmesa.ingest.util

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql._


class Caching(bucket: String, key: String) {
  private val s3client = AmazonS3ClientBuilder.defaultClient()

  private def s3key(filename: String): String = s"${prefix}/cache/${filename}"

  private def s3uri(filename: String): String = s"${bucket}/${s3key(filename)}"

  private def s3exists(filename: String): Boolean = s3client.doesObjectExist(bucket, s3key(filename))

  def orc(filename: String)(sparkjob: () => DataFrame)(implicit ss: SparkSession): DataFrame = {
    if (s3exists(filename)) {
      ss.read.orc(s3uri(filename))
    } else {
      val df = sparkjob()
      df.write.format("orc").save(s3Uri(filename))
      df
    }
  }
}
