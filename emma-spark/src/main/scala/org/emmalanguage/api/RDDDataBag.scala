package org.emmalanguage
package api

import org.apache.spark.rdd._

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
 * A `DataBag` implementation backed by a Scala Seq.
 */
class RDDDataBag[A: ClassTag] private[api](private val rep: RDD[A]) extends DataBag[A] {

  import RDDDataBag.wrap

  override final type Self[X] = RDDDataBag[X]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: ClassTag](z: B)(s: A => B, p: (B, B) => B): B =
    rep.map(x => s(x)).fold(z)(p)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: ClassTag](f: (A) => B): Self[B] =
    rep.map(f)

  override def flatMap[B: ClassTag](f: (A) => Self[B]): Self[B] =
    rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): Self[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping and Set operations
  // -----------------------------------------------------

  override def groupBy[K: ClassTag](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    rep.groupBy(k).map { case (key, vals) => Group(key, DataBag(vals.toSeq)) }

  override def plus(addend: Self[A]): DataBag[A] =
    this.rep union addend.rep

  override def distinct(): DataBag[A] =
    rep.distinct

  // -----------------------------------------------------
  // Conversion Methods
  // -----------------------------------------------------

  def fetch(): Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  // override def equals(o: Any) = ??? TODO

  // override def hashCode(): Int = ??? TODO

  // override def toString: String = ??? TODO
}

object RDDDataBag {

  private implicit def wrap[A: ClassTag](rep: RDD[A]): RDDDataBag[A] =
    new RDDDataBag(rep)

  def main(args: Array[String]): Unit = {
    val xs = new SeqDataBag(Seq(1, 2, 3))
    val ys = new SeqDataBag(Seq(2, 4, 5))

    xs.flatMap(x => ys.map(y => println(x, y)))

    for {
      x <- xs
      y <- ys
    } yield println((x, y))
  }
}