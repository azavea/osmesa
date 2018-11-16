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

          val traits = traitsIn(tag.tpe).sortBy(_.typeSymbol.name.toString)

          val name = pkg + "." + traits
            .map(t => {
              List(t.typeSymbol.name.toString,
                   t.typeArgs.map(x => x.typeSymbol.name.toString).mkString("With"))
                .filter(!_.isEmpty)
                .mkString("Of")
            })
            .mkString("With")

          // https://stackoverflow.com/a/23792152/507685
          // confirm that a concrete case class implementation exists
          try {
            Class.forName(name) // obtain java.lang.Class object from a string
          } catch {
            case e: ClassNotFoundException =>
              throw new RuntimeException(s"${e.getMessage} must be an implementation of ${traits
                .mkString(", ")}")
            case e: Throwable => throw e
          }

          // obtain a runtime mirror, using the current thread's context classloader; this is used rather than the
          // class's classloader (Class.forName, above) in order to work with the Scala REPL (which uses a different
          // ClassLoader instance than was used to load classes in the first place); if Class.getClassLoader() is used,
          // this mirror won't match TypeCreator's mirror, below
          val mirror = runtimeMirror(Thread.currentThread().getContextClassLoader)

          // get a Type object
          val tpe = mirror.staticClass(name).selfType

          // create a type tag which contains the above type object
          val targetType = TypeTag(
            mirror,
            new api.TypeCreator {
              def apply[U <: api.Universe with Singleton](m: api.Mirror[U]): U#Type =
                if (m == mirror) tpe.asInstanceOf[U#Type]
                else
                  throw new IllegalArgumentException(
                    s"Type tag defined in $mirror cannot be migrated to other mirrors, specifically $m.")
            }
          ).asInstanceOf[TypeTag[Product]]

          Encoders.product(targetType)
        }
      )
      .asInstanceOf[Encoder[T]]
  }

  private[osmesa] def traitsIn(tpe: Type): List[Type] = {
    tpe match {
      case rt: RefinedType => rt.parents.flatMap(p => traitsIn(p))
      case _               => List(tpe)
    }
  }
}
