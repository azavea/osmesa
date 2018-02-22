package osmesa.ingest.util

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql._

import java.io.File


trait Caching {
  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame
}

class S3Caching(bucket: String, prefix: String) extends Caching {
  private val s3client = AmazonS3ClientBuilder.defaultClient()

  private def s3key(filename: String): String = s"${prefix}/cache/${filename}"

  private def s3uri(filename: String): String = s"s3://${bucket}/${s3key(filename)}"

  private def s3exists(filename: String): Boolean = s3client.doesObjectExist(bucket, s3key(filename))

  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame = {
    if (s3exists(filename)) {
      ss.read.orc(s3uri(filename))
    } else {
      sparkjob.repartition(1).write.format("orc").save(s3uri(filename))
      sparkjob
    }
  }
}

class FsCaching(path: String = "") extends Caching {
  private def f(filename: String) = new File(path, s"cache/${filename}")

  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame = {
    if (f(filename).exists()) {
      ss.read.orc(f(filename).toURI.toString)
    } else {
      sparkjob.repartition(1).write.format("orc").save(f(filename).toURI.toString)
      sparkjob
    }
  }
}

object Caching {

  def onS3(bucket: String, prefix: String) = new S3Caching(bucket, prefix)

  def onFs(relPath: String = "") = new FsCaching(relPath)

}

