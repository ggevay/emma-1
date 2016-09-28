package org.emmalanguage
package api

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.{ClassTag, classTag}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * A `DataBag` implementation backed by a Scala `Traversable`.
 */
class ScalaTraversable[A] private[api](private val rep: Traversable[A]) extends DataBag[A] {

  override type TypeClass[_] = Unit

  import ScalaTraversable.{wrap, ioSupport}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta : ClassTag : TypeTag: WeakTypeTag: TypeClass](z: B)(s: A => B, p: (B, B) => B): B =
    rep.foldLeft(z)((acc, x) => p(s(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta : ClassTag : TypeTag](f: (A) => B): DataBag[B] =
    rep.map(f)


  override def flatMap[B: Meta : ClassTag : TypeTag](f: (A) => DataBag[B]): DataBag[B] =
    rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): DataBag[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta : ClassTag : TypeTag](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    rep.groupBy(k).toSeq.map { case (key, vals) => Group(key, wrap(vals)) }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case rdd: ScalaTraversable[A] => this.rep union rdd.rep
  }

  override def distinct: DataBag[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def write[F <: io.Format : Meta : ClassTag : TypeTag](path: String, options: F#Config): Unit =
    ioSupport[F].write(path)(rep)

  override def write[F <: io.Format : Meta : ClassTag : TypeTag](path: String): Unit =
    ioSupport[F].write(path)(rep)

  override def fetch(): Traversable[A] =
    rep

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any) = o match {
    case that: ScalaTraversable[A] =>
      lazy val sizeEq = this.rep.size == that.rep.size
      lazy val diffEm = (this.rep.toSeq diff this.rep.toSeq).isEmpty
      sizeEq && diffEm
    case _ =>
      false
  }

  override def hashCode(): Int =
    scala.util.hashing.MurmurHash3.unorderedHash(rep)

  override def toString: String =
    rep.toString
}

object ScalaTraversable {

  private implicit def wrap[A](rep: Traversable[A]): ScalaTraversable[A] =
    new ScalaTraversable(rep)

  private implicit def ioSupport[F <: io.Format : Meta : ClassTag : TypeTag]: io.Support[F] = {
    // val ct = implicitly[ClassTag[F]]
    ???
  }

  implicit def implUnit: Unit = ()

  def apply[A: Meta : ClassTag : TypeTag]: ScalaTraversable[A] =
    new ScalaTraversable(Seq.empty)

  def apply[A: Meta : ClassTag : TypeTag](values: Traversable[A]): ScalaTraversable[A] =
    new ScalaTraversable(values)
}