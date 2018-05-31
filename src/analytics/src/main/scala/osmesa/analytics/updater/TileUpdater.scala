package osmesa.analytics.updater

import java.io.File
import java.net.URI
import java.nio.file.Path

import cats.implicits._
import com.monovore.decline._
import org.apache.log4j.Logger
import osmesa.analytics.updater.schemas._

class TileUpdater

object TileUpdater
    extends CommandApp(
      name = "update-tiles",
      header = "Update vector tiles with changes from an augmented diff",
      main = {
        val rootURI = new File("").toURI

        val replicationSourceOpt = Opts
          .option[URI](
            "replication-source",
            short = "r",
            metavar = "uri",
            help = "URI prefix for replication files"
          )
          .withDefault(rootURI)
        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix for vector tiles to update"
          )
          .withDefault(rootURI)
        val minZoomOpt = Opts.option[Int](
          "min-zoom",
          short = "z",
          metavar = "zoom",
          help = "Minimum zoom to consider"
        )
        val maxZoomOpt = Opts.option[Int](
          "max-zoom",
          short = "Z",
          metavar = "zoom",
          help = "Maximum zoom to consider"
        )
        val schemaOpt = Opts
          .option[String](
            "schema",
            short = "s",
            metavar = "schema",
            help = "Schema"
          )
          .withDefault("snapshot")
          .validate("Must be a registered schema") {
            Schemas.keySet.contains(_)
          }
          .map {
            Schemas(_)
          }
        val listingOpt = Opts
          .option[Path](
            "tiles",
            short = "T",
            metavar = "tile list",
            help = "List of tiles available for updating"
          )
          .orNone
        val dryRunOpt = Opts
          .flag(
            "dry-run",
            short = "n",
            help = "Dry run"
          )
          .orFalse
        val verboseOpt = Opts
          .flag(
            "verbose",
            short = "v",
            help = "Be verbose"
          )
          .orFalse
        val sequenceOpt = Opts.argument[Int]("sequence")

        val logger = Logger.getLogger(classOf[TileUpdater])

        (replicationSourceOpt,
         tileSourceOpt,
         minZoomOpt,
         maxZoomOpt,
         schemaOpt,
         listingOpt,
         dryRunOpt,
         verboseOpt,
         sequenceOpt).mapN {
          (replicationSource,
           tileSource,
           minZoom,
           maxZoom,
           schema,
           listing,
           dryRun,
           verbose,
           sequence) =>
            val replicationUri = replicationSource.resolve(s"$sequence.json")

            if (verbose) {
              println(s"Applying $replicationUri to $tileSource from zoom $minZoom to $maxZoom...")
            }

            readFeatures(replicationUri) match {
              case Some(features) =>
                for (zoom <- minZoom to maxZoom) {
                  updateTiles(
                    tileSource = tileSource,
                    zoom = zoom,
                    schemaType = schema,
                    features = features,
                    listing = listing,
                    process = (sk, tile) => {
                      val filename = s"$zoom/${sk.col}/${sk.row}.mvt"
                      val uri = tileSource.resolve(filename)

                      if (dryRun) {
                        println(
                          s"Would write ${tile.toBytes.length.formatted("%,d")} bytes to $uri")
                      } else {
                        logger.info(
                          s"Writing ${tile.toBytes.length.formatted("%,d")} bytes to $uri")
                        write(uri, tile.toBytes)
                      }

                      if (verbose) {
                        println(filename)
                      }
                    }
                  )
                }

              case None =>
                println(s"No features available for $sequence")
                System.exit(1)
            }
        }
      }
    )
