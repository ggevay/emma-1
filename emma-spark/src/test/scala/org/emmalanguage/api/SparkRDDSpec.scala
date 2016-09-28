package org.emmalanguage
package api

import eu.stratosphere.emma.testschema.Literature._

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class SparkRDDSpec extends FreeSpec with Matchers with PropertyChecks with DataBagEquality {

  import LocalSparkSession._

  "structural recursion" in {
    withSparkContext { implicit sc =>
      val act = {
        val xs = SparkRDD(hhCrts)
        val ys = SparkRDD(hhCrts.map(_.book.title.length))

        // FIXME: these predicates have to be defined externally in order to be a serializable part of the fold closure
        // FIXME: However, hard-coding the expanded version of the problematic folds (find, count) resolves the issue
        val p1 = (c: Character) => c.name startsWith "Zaphod"
        val p2 = (c: Character) => c.name startsWith "Ford"
        val p3 = (c: Character) => c.name startsWith "Marvin"

        Seq(
          //@formatter:off
          "isEmpty"  -> xs.isEmpty,
          "nonEmpty" -> xs.nonEmpty,
          "min"      -> ys.min,
          "max"      -> ys.max,
          "sum"      -> ys.sum,
          "product"  -> ys.product,
          "size"     -> xs.size,
          "count"    -> xs.count(p1), // FIXME: fold macro needs externally defiend lambda
          "existsP"  -> xs.exists(_.name startsWith "Arthur"),
          "existsN"  -> xs.exists(_.name startsWith "Marvin"),
          "forallP"  -> xs.forall(_.name startsWith "Arthur"),
          "forallN"  -> xs.forall(_.name startsWith "Trillian"),
          "findP"    -> xs.find(p2), // FIXME: fold macro needs externally defiend lambda
          "findN"    -> xs.find(p3), // FIXME: fold macro needs externally defiend lambda
          "bottom"   -> ys.bottom(1),
          "top"      -> ys.top(2)
          //@formatter:on
        )
      }

      val exp = {
        val xs = hhCrts
        val ys = hhCrts.map(_.book.title.length)
        Seq(
          //@formatter:off
          "isEmpty"  -> xs.isEmpty,
          "nonEmpty" -> xs.nonEmpty,
          "min"      -> ys.min,
          "max"      -> ys.max,
          "sum"      -> ys.sum,
          "product"  -> ys.product,
          "size"     -> xs.size,
          "count"    -> xs.count(_.name startsWith "Zaphod"),
          "existsP"  -> xs.exists(_.name startsWith "Arthur"),
          "existsN"  -> xs.exists(_.name startsWith "Marvin"),
          "forallP"  -> xs.forall(_.name startsWith "Arthur"),
          "forallN"  -> xs.forall(_.name startsWith "Trillian"),
          "findP"    -> xs.find(_.name startsWith "Ford"),
          "findN"    -> xs.find(_.name startsWith "Marvin"),
          "bottom"   -> ys.sorted.slice(ys.length - 1, ys.length),
          "top"      -> ys.sorted.slice(0, 2)
          //@formatter:on
        )
      }

      act should have size exp.size

      for ((k, v) <- exp)
        act should contain(k, v)
    }
  }

  "monad ops" - {

    "map" in {
      withSparkContext { implicit sc =>
        val act = SparkRDD(hhCrts)
          .map(c => c.name)

        val exp = hhCrts
          .map(c => c.name)

        act shouldEqual DataBag(exp)
      }
    }

  "flatMap" in {
    withSparkContext { implicit sc =>
      val act = SparkRDD(Seq((hhBook, hhCrts)))
        .flatMap { case (b, cs) => DataBag(cs) }

      val exp = Seq((hhBook, hhCrts))
        .flatMap { case (b, cs) => cs }

      act shouldEqual DataBag(exp)
    }
    }

    "withFilter" in {
      withSparkContext { implicit spark =>
        val act = SparkRDD(Seq(hhBook))
          .withFilter(_.title == "The Hitchhiker's Guide to the Galaxy")

        val exp = Seq(hhBook)
          .filter(_.title == "The Hitchhiker's Guide to the Galaxy")

        act shouldEqual DataBag(exp)
      }
    }

    "for-comprehensions" in {
      withSparkContext { implicit sc =>
        val act = for {
          b <- SparkRDD(Seq(hhBook))
          c <- ScalaTraversable(hhCrts) // nested DataBag cannot be RDDDataBag, as those are not serializable
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
  }

  "grouping" in {
    withSparkContext { implicit sc =>
      val act = SparkRDD(hhCrts).groupBy(_.book)

      val exp = hhCrts.groupBy(_.book).toSeq.map {
        case (k, vs) => Group(k, DataBag(vs))
      }

      act shouldEqual DataBag(exp)
    }
  }

  "set operations" - {

    val xs = Seq("foo", "bar", "baz", "boo", "buz", "baz", "bag")
    val ys = Seq("fuu", "bin", "bar", "bur", "lez", "liz", "lag")

    "union" in {
      withSparkContext { implicit sc =>
        val act = SparkRDD(xs) union SparkRDD(ys)
        val exp = xs union ys

        act shouldEqual DataBag(exp)
      }
    }

    "distinct" in {
      withSparkContext { implicit sc =>
        val acts = Seq(SparkRDD(xs).distinct, SparkRDD(ys).distinct)
        val exps = Seq(xs.distinct, ys.distinct)

        for ((act, exp) <- acts zip exps)
          act shouldEqual SparkRDD(exp)
      }
    }
  }

  "sinks" - {
  }
}
