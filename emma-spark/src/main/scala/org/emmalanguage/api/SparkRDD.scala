package org.emmalanguage
package api

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
 * A `DataBag` implementation backed by a Spark `RDD`.
 */
class SparkRDD[A: Meta] private[api](private val rep: RDD[A]) extends DataBag[A] {

  import SparkRDD.wrap
  import MetaImplicits._

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B =
    rep.map(x => s(x)).fold(z)(u)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): DataBag[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    rep.groupBy(k).map { case (key, vals) => Group(key, DataBag(vals.toSeq)) }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case rdd: SparkRDD[A] => this.rep union rdd.rep
  }

  override def distinct: DataBag[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def write[F <: io.Format : Meta](path: String, options: F#Config): Unit =
    ???

  override def write[F <: io.Format : Meta](path: String): Unit =
    ???

  def fetch(): Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  // override def equals(o: Any) = ??? TODO

  // override def hashCode(): Int = ??? TODO

  // override def toString: String = ??? TODO
}

object SparkRDD {

  import MetaImplicits._

  private implicit def wrap[A: Meta](rep: RDD[A]): SparkRDD[A] =
    new SparkRDD(rep)

  def apply[A: Meta]()(implicit sc: SparkContext): SparkRDD[A] =
    sc.emptyRDD[A]

  def apply[A: Meta](seq: Seq[A])(implicit sc: SparkContext): SparkRDD[A] =
    sc.parallelize(seq)
}