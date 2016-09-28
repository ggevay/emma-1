package org.emmalanguage
package api

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import scala.language.{higherKinds, implicitConversions}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * A `DataBag` implementation backed by a Spark `RDD`.
 */
class SparkRDD[A: Meta : ClassTag : TypeTag] private[api](private val rep: RDD[A]) extends DataBag[A] {

  import SparkRDD.wrap

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta : ClassTag : TypeTag](z: B)(s: A => B, u: (B, B) => B): B =
    rep.map(x => s(x)).fold(z)(u)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta : ClassTag : TypeTag](f: (A) => B): DataBag[B] =
    rep.map(f)

  override def flatMap[B: Meta : ClassTag : TypeTag](f: (A) => DataBag[B]): DataBag[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): DataBag[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta : ClassTag : TypeTag](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
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

  def write[F <: io.Format : Meta : ClassTag : TypeTag](path: String, options: F#Config): Unit =
    ???

  override def write[F <: io.Format : Meta : ClassTag : TypeTag](path: String): Unit =
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

  implicit def wrap[A: Meta : ClassTag : TypeTag](rep: RDD[A]): SparkRDD[A] =
    new SparkRDD(rep)

  def apply[A: Meta : ClassTag : TypeTag]()(implicit sc: SparkContext): SparkRDD[A] =
    sc.emptyRDD[A]

  def apply[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit sc: SparkContext): SparkRDD[A] =
    sc.parallelize(seq)
}