package org.emmalanguage
package api

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.{higherKinds, implicitConversions}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta : ClassTag : TypeTag] private[api](private val rep: Dataset[A]) extends DataBag[A] {

  import SparkDataset.{encoderForType, wrap}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta : ClassTag : TypeTag: WeakTypeTag](z: B)(s: A => B, u: (B, B) => B): B =
    rep.map(x => s(x)).reduce(u) // TODO: handle the empty Dataset case (maybe catch the exception?)

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
    rdd.groupBy(k)

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dataset: SparkDataset[A] => this.rep union dataset.rep
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
  // Conversions
  // -----------------------------------------------------

  def rdd: SparkRDD[A] =
    new SparkRDD(rep.rdd)
}

object SparkDataset {

  import org.apache.spark.sql.Encoder
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

  implicit def encoderForType[T: Meta : ClassTag : TypeTag]: Encoder[T] =
    ExpressionEncoder[T]

  implicit def wrap[A: Meta : ClassTag : TypeTag](rep: Dataset[A]): SparkDataset[A] =
    new SparkDataset(rep)

  def apply[A: Meta : ClassTag : TypeTag]()(implicit spark: SparkSession): SparkDataset[A] =
    spark.emptyDataset[A]

  def apply[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit spark: SparkSession): SparkDataset[A] =
    spark.createDataset(seq)
}
