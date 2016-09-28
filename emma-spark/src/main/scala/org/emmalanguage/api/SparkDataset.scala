package org.emmalanguage
package api

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta] private[api](private val rep: Dataset[A]) extends DataBag[A] {

  import SparkDataset.{encoderForType, wrap}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B =
    rep.map(x => s(x)).reduce(u)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): DataBag[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
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

  def write[F <: io.Format : Meta](path: String, options: F#Config): Unit =
    ???

  override def write[F <: io.Format : Meta](path: String): Unit =
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

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  implicit def wrap[A: Meta](rep: Dataset[A]): SparkDataset[A] =
    new SparkDataset(rep)

  def apply[A: Meta]()(implicit spark: SparkSession): SparkDataset[A] =
    spark.emptyDataset[A]

  def apply[A: Meta](seq: Seq[A])(implicit spark: SparkSession): SparkDataset[A] =
    spark.createDataset(seq)
}
