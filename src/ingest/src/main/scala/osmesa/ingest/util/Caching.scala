package osmesa.ingest.util

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql._

import java.io.File


trait Caching {
  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession, cachePartitions: Option[Int] = None): DataFrame
}

class S3Caching(cacheDir: String) extends Caching {
  private val s3client = AmazonS3ClientBuilder.defaultClient()

  private val bucket = cacheDir.split("/")(2)

  private def prefix(filename: String): String =
    cacheDir.split("/").drop(3).mkString("/") + "/" + filename

  private def fileUri(filename: String): String =
    cacheDir + "/" + filename

  private def s3exists(filename: String): Boolean =
    s3client.doesObjectExist(bucket, prefix(filename) + "/_SUCCESS")

  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession, cachePartitions: Option[Int] = None): DataFrame = {
    if (!s3exists(filename)) {
      sparkjob.repartition(cachePartitions.getOrElse(100)).write.mode(SaveMode.Overwrite).format("orc").save(fileUri(filename))
    }

    ss.read.orc(fileUri(filename))
  }
}

class FsCaching(cacheDir: String) extends Caching {
  private def f(filename: String) = new File(cacheDir, filename)

  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession, cachePartitions: Option[Int] = None): DataFrame = {
    if (!f(filename).exists()) {
      sparkjob.repartition(cachePartitions.getOrElse(1)).write.mode(SaveMode.Overwrite).format("orc").save(f(filename).toURI.toString)
    }

    try {
      ss.read.orc(f(filename).toURI.toString)
    } catch {
      case e: Throwable =>
        println(filename)
        throw e
    }
  }
}

object NoCaching extends Caching {
  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession, cachePartitions: Option[Int] = None): DataFrame = sparkjob
}


object Caching {

  def onS3(cacheDir: String) = new S3Caching(cacheDir)

  def onFs(cacheDir: String) = new FsCaching(cacheDir)

  def none = NoCaching

}

