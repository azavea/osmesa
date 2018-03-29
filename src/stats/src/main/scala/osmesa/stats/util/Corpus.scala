package osmesa.stats.util

import osmesa.common.model._

import cats._
import cats.implicits._
import scalaj.http._


object Corpus {
  lazy val firstNames =
    Http("https://www.cs.cmu.edu/Groups/AI/areas/nlp/corpora/names/other/names.txt").asString.body.split("\n")

  lazy val lastNames =
    Http("https://www.cs.cmu.edu/Groups/AI/areas/nlp/corpora/names/other/names.txt").asString.body.split("\n")

  lazy val countries =
    Http("http://www.countries-list.info/Download-List/download").asString.body.split("\n").map(_.split(":").tail.head)

  def randomName = (firstNames.takeRandom, lastNames.takeRandom).mapN({ (fName, lName) =>
    fName + " " + lName
  }).getOrElse("John Smith")

  def randomCountry = countries.takeRandom.getOrElse("USA")
}

