package org.emmalanguage
package api

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
 * A `DataBag` implementation backed by a Scala Seq.
 */
class SeqDataBag[A] private[api](private val rep: Seq[A]) extends DataBag[A] {

  import SeqDataBag.wrap

  override final type Self[X] = SeqDataBag[X]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: ClassTag](z: B)(s: A => B, p: (B, B) => B): B =
    rep.foldLeft(z)((acc, x) => p(s(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: ClassTag](f: (A) => B): Self[B] =
    rep.map(f)

  override def flatMap[B: ClassTag](f: (A) => Self[B]): Self[B] =
    rep.flatMap(x => f(x).rep)

  def withFilter(p: (A) => Boolean): Self[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping and Set operations
  // -----------------------------------------------------

  override def groupBy[K: ClassTag](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    rep.groupBy(k).toSeq.map { case (key, vals) => Group(key, wrap(vals)) }

  override def plus(addend: Self[A]): DataBag[A] =
    this.rep ++ addend.rep

  override def distinct(): DataBag[A] =
    rep.distinct

  // -----------------------------------------------------
  // Conversion Methods
  // -----------------------------------------------------

  override def fetch(): Seq[A] =
    rep

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any) = o match {
    case that: SeqDataBag[A] =>
      lazy val sizeEq = this.rep.size == that.rep.size
      lazy val diffEm = (this.rep diff this.rep).isEmpty
      sizeEq && diffEm
    case _ =>
      false
  }

  override def hashCode(): Int =
    scala.util.hashing.MurmurHash3.unorderedHash(rep)

  override def toString: String =
    rep.toString
}

object SeqDataBag {

  private implicit def wrap[A](rep: Seq[A]): SeqDataBag[A] =
    new SeqDataBag(rep)

}