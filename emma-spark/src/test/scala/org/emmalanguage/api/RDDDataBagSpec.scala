package org.emmalanguage
package api

import eu.stratosphere.emma.testschema.Literature._

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class RDDDataBagSpec extends FreeSpec with Matchers with PropertyChecks with DataBagEquality {

  import LocalSparkSession._

  "maps" in {
    withSparkContext { implicit sc =>
      val act = for {
        c <- RDDDataBag(hhCrts)
      } yield c.name

      val exp = for {
        c <- hhCrts
      } yield c.name

      act shouldEqual DataBag(exp)
    }
  }

  "flatMap" in {
    withSparkContext { implicit sc =>
      val act = for {
        (b, cs) <- RDDDataBag(Seq((hhBook, DataBag(hhCrts))))
        c <- cs
      } yield c.name

      val exp = for {
        (b, cs) <- Seq((hhBook, hhCrts))
        c <- cs
      } yield c.name

      act shouldEqual DataBag(exp)
    }
  }

  "withFilter" in {
    withSparkContext { implicit sc =>
      val act = RDDDataBag(Seq(hhBook))
        .withFilter(_.title == "The Hitchhiker's Guide to the Galaxy")

      val exp = Seq(hhBook)
        .filter(_.title == "The Hitchhiker's Guide to the Galaxy")

      act shouldEqual DataBag(exp)
    }
  }

  "for-comprehensions" in {
    withSparkContext { implicit sc =>
      val act = for {
        b <- RDDDataBag(Seq(hhBook))
        c <- SeqDataBag(hhCrts) // nested DataBag cannot be RDDDataBag, as those are not serializable
        if b.title == c.book.title
        if b.title == "The Hitchhiker's Guide to the Galaxy"
      } yield (b.title, c.name)

      val exp = for {
        b <- Seq(hhBook)
        c <- hhCrts
        if b.title == c.book.title
        if b.title == "The Hitchhiker's Guide to the Galaxy"
      } yield (b.title, c.name)

      act shouldEqual DataBag(exp)
    }
  }

  "groupBy" in {
    withSparkContext { implicit sc =>
      val act = RDDDataBag(hhCrts).groupBy(_.book)

      val exp = hhCrts.groupBy(_.book).toSeq.map {
        case (k, vs) => Group(k, DataBag(vs))
      }

      act shouldEqual DataBag(exp)
    }
  }

}
