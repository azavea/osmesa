package osmesa.analytics

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Schemas {
  /*
  CREATE EXTERNAL TABLE planet_history (
      id BIGINT,
      type STRING,
      tags MAP<STRING,STRING>,
      lat DECIMAL(9,7),
      lon DECIMAL(10,7),
      nds ARRAY<STRUCT<ref: BIGINT>>,
      members ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
      changeset BIGINT,
      timestamp TIMESTAMP,
      uid BIGINT,
      user STRING,
      version BIGINT,
      visible BOOLEAN
  )
   */

  val history =
    StructType(
      Seq(
        StructField("id", LongType, false),
        StructField("type", StringType, false),
        StructField("tags", MapType(StringType, StringType), false),
        StructField("lat", DoubleType, false),
        StructField("lon", DoubleType, false),
        StructField(
          "nds",
          ArrayType(
            StructType(
              StructField("ref", LongType, false) :: Nil
            )
          ),
          true
        ),
        StructField(
          "members",
          ArrayType(
            StructType(
              Seq(
                StructField("type", StringType, false),
                StructField("ref", LongType, false),
                StructField("role", StringType, true)
              )
            )
          ),
          true
        ),
        StructField("changeset", LongType, false),
        StructField("timestamp", TimestampType, false),
        StructField("uid", LongType, false),
        StructField("user", StringType, false),
        StructField("version", LongType, false),
        StructField("visible", BooleanType, false)
      )
    )


  /*
  CREATE EXTERNAL TABLE changesets (
      id BIGINT,
      tags MAP<STRING,STRING>,
      created_at TIMESTAMP,
      open BOOLEAN,
      closed_at TIMESTAMP,
      comments_count BIGINT,
      min_lat DECIMAL(9,7),
      max_lat DECIMAL(9,7),
      min_lon DECIMAL(10,7),
      max_lon DECIMAL(10,7),
      num_changes BIGINT,
      uid BIGINT,
      user STRING
  )
   */

  val changesets =
    StructType(
      Seq(
        StructField("id", LongType, false),
        StructField("tags", MapType(StringType, StringType), false),
        StructField("created_at", TimestampType, false),
        StructField("open", BooleanType, false),
        StructField("closed_at", TimestampType, false),
        StructField("comments_count", LongType, false),
        StructField("min_lat", DoubleType, false),
        StructField("max_lat", DoubleType, false),
        StructField("min_lon", DoubleType, false),
        StructField("max_lon", DoubleType, false),
        StructField("num_changes", LongType, false),
        StructField("uid", LongType, false),
        StructField("user", StringType, false)
      )
    )
}
