package osmesa.analytics.oneoffs

import geotrellis.spark.io.s3.S3Client
import io.circe._
import io.circe.syntax._

import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata, PutObjectRequest, PutObjectResult}
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.{Process, nondeterminism}

import java.io.ByteArrayInputStream
import java.util.concurrent.Executors
import scala.reflect.ClassTag

object JsonRDDWriter {
  def getS3Client: () => S3Client = () => S3Client.DEFAULT

  def write[V: Encoder: ClassTag](
    rdd: RDD[V],
    bucket: String,
    keyPath: V => String,
    putObjectModifier: PutObjectRequest => PutObjectRequest = { p => p },
    threads: Int = 16
  ): Unit = {

    val _getS3Client = getS3Client

    val pathsToValues =
      rdd.
        map { v => (keyPath(v), v) }

    pathsToValues.foreachPartition { partition =>
      if(partition.nonEmpty) {
        import geotrellis.spark.util.TaskUtils._
        val getS3Client = _getS3Client
        val s3client: S3Client = getS3Client()

        val requests: Process[Task, PutObjectRequest] =
          Process.unfold(partition) { iter =>
            if (iter.hasNext) {
              val recs = iter.next()
              val key = recs._1
              val bytes = recs._2.asJson.spaces2.getBytes("utf-8")
              val metadata = new ObjectMetadata()
              metadata.setContentLength(bytes.length)
              metadata.setContentType("application/json")
              val is = new ByteArrayInputStream(bytes)
              val request = putObjectModifier(new PutObjectRequest(bucket, key, is, metadata))
              Some(request, iter)
            } else {
              None
            }
          }

        val pool = Executors.newFixedThreadPool(threads)

        val write: PutObjectRequest => Process[Task, PutObjectResult] = { request =>
          Process eval Task {
            request.getInputStream.reset() // reset in case of retransmission to avoid 400 error
            s3client.putObject(request)
          }(pool).retryEBO {
            case e: AmazonS3Exception if e.getStatusCode == 503 => true
            case _ => false
          }
        }

        val results = nondeterminism.njoin(maxOpen = threads, maxQueued = threads) { requests map write }(Strategy.Executor(pool))
        results.run.unsafePerformSync
        pool.shutdown()
      }
    }
  }
}
