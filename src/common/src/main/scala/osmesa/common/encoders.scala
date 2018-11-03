package osmesa.common

import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable
import scala.reflect.api
import scala.reflect.runtime.universe.{TypeTag, _}

object encoders {
  private val EncoderCache: mutable.Map[TypeTag[_], Encoder[_]] =
    mutable.Map.empty[TypeTag[_], Encoder[_]]

  private[osmesa] def buildEncoder[T](implicit tag: TypeTag[T]): Encoder[T] = {
    EncoderCache
      .getOrElseUpdate(
        tag, {
          val pkg = "osmesa.common.impl"

          val traits = traitsIn[T]
          val traitNames = traits.map(_.name.toString).toSeq.sorted

          val name = s"$pkg.${traitNames.mkString("With")}"

          // https://stackoverflow.com/a/23792152/507685
          val c = try {
            Class.forName(name) // obtain java.lang.Class object from a string
          } catch {
            case e: ClassNotFoundException =>
              throw new RuntimeException(
                s"${e.getMessage} must be an implementation of the following traits: ${traitNames
                  .mkString(", ")}")
            case e: Throwable => throw e
          }

          val mirror = runtimeMirror(c.getClassLoader) // obtain runtime mirror
          val sym = mirror.staticClass(name) // obtain class symbol for `c`
          val tpe = sym.selfType // obtain type object for `c`

          // create a type tag which contains the above type object
          val targetType = TypeTag(
            mirror,
            new api.TypeCreator {
              def apply[U <: api.Universe with Singleton](m: api.Mirror[U]): U#Type =
                if (m == mirror) tpe.asInstanceOf[U#Type]
                else
                  throw new IllegalArgumentException(
                    s"Type tag defined in $mirror cannot be migrated to other mirrors.")
            }
          ).asInstanceOf[TypeTag[Product]]

          Encoders.product(targetType)
        }
      )
      .asInstanceOf[Encoder[T]]
  }

  // determine closest traits
  private def traitsIn[T](implicit tag: TypeTag[T]): Set[TypeSymbol] = {
    val tpe = tag.tpe

    val t = tpe.baseClasses.filter(s => s.isAbstract && s != typeOf[Any].typeSymbol).map(_.asType)

    t.foldLeft(Seq.empty[TypeSymbol]) {
        case (acc, x) => {
          // if x is a super type of anything in acc, skip it
          if (acc.exists(y => y.toType <:< x.toType)) {
            acc
          } else {
            // filter out anything in acc that's a super type of x
            acc.filterNot(y => x.toType <:< y.toType) :+ x
          }
        }
      }
      .sortBy(_.toString)
      .toSet
  }
}
