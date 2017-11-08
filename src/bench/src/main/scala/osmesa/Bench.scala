package osmesa

import java.util.concurrent.TimeUnit

import scala.util.Try

import cats.implicits._
import org.apache.log4j
import org.apache.spark._
import org.apache.spark.sql._
import org.openjdk.jmh.annotations._
import osmesa.analytics.oneoffs.Analysis

// --- //

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
class Bench {

  var conf: SparkConf = _
  implicit var ss: SparkSession = _

  @Setup
  def setup: Unit = {
    conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("road-changes")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

    ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

    /* Silence the damn INFO logger */
    log4j.Logger.getRootLogger().setLevel(log4j.Level.ERROR)
  }

  @TearDown
  def close: Unit = ss.stop()

  @Benchmark
  def roads: Try[Double] = {
    val path: String = "/home/colin/code/azavea/vectorpipe/data/isle-of-man.orc"

    Try(ss.read.orc(path)) >>= Analysis.roads
  }

}
