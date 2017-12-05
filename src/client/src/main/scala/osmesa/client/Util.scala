package osmesa.client

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.apache.spark._
import org.apache.spark.sql._
import cats.data.{NonEmptyList => NEL, _}
import cats.data.Validated._
import cats.syntax._

import scala.util.Try
import java.net._
import java.io._


object Util {
  def UriToInputStream(uri: URI): Option[InputStream] = uri.getScheme match {
    case "file" =>
      Try(new FileInputStream(new File(uri.getPath))).toOption
    case "https" | "http" =>
      Try(uri.toURL.openStream).toOption
    case _ =>
      Try(new FileInputStream(new File(uri.toString))).toOption
  }

  def loadYamlAsDatastoreConf(uri: URI): Validated[NEL[String], java.util.Map[String, String]] =
    UriToInputStream(uri).flatMap({ stream =>
      Try(new Yaml().load(stream).asInstanceOf[java.util.Map[String, String]]).toOption
    }) match {
      case Some(dsConf) => Valid(dsConf)
      case None => Invalid(NEL.of("Unable to load YAML datastore configuration"))
    }

  def loadOrcAsDataFrame(ss: SparkSession, uri: URI) = {
    println("URI", uri.toString)
    ss.read.orc(uri.toString)
    Try(ss.read.orc(uri.toString)).toOption match {
      case Some(df) => Valid(df)
      case None => Invalid(NEL.of("Unable to load Orc as Spark DataFrame"))
    }
  }
}
