package osmesa.ingest.util

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql._

import java.io.File


trait Caching {
  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame
}

class S3Caching(cacheDir: String) extends Caching {
  private val s3client = AmazonS3ClientBuilder.defaultClient()

  private val bucket = cacheDir.split("/")(2)

  private def prefix(filename: String): String =
    cacheDir.split("/").drop(3).mkString("/") + "/" + filename

  private def fileUri(filename: String): String =
    cacheDir + "/" + filename

  private def s3exists(filename: String): Boolean =
    s3client.doesObjectExist(bucket, prefix(filename))

  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame = {
    println("in cache func")
    println("bucket", bucket)
    println(prefix(filename))
    println(fileUri(filename))
    println("s3exists", s3exists(filename))
    if (s3exists(filename)) {
      ss.read.orc(fileUri(filename))
    } else {
      sparkjob.repartition(1).write.format("orc").save(fileUri(filename))
      sparkjob
    }
  }
}

class FsCaching(cacheDir: String) extends Caching {
  private def f(filename: String) = new File(cacheDir, filename)

  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame = {
    if (f(filename).exists()) {
      ss.read.orc(f(filename).toURI.toString)
    } else {
      sparkjob.repartition(1).write.format("orc").save(f(filename).toURI.toString)
      sparkjob
    }
  }
}

object NoCaching extends Caching {
  def orc(filename: String)(sparkjob: => DataFrame)(implicit ss: SparkSession): DataFrame = sparkjob
}


object Caching {

  def onS3(cacheDir: String) = new S3Caching(cacheDir)

  def onFs(cacheDir: String) = new FsCaching(cacheDir)

  def none = NoCaching

}

