package org.emmalanguage
package api

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.runtime.universe._

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta] private[api](private val rep: Dataset[A]) extends DataBag[A] {

  @transient implicit private def spark = rep.sparkSession

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B =
    ??? // rep.map(x => s(x)).reduce(u)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    ??? // rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    ??? // rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): DataBag[A] =
    ??? // rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    ??? // rep.groupBy(k).map { case (key, vals) => Group(key, DataBag(vals.toSeq)) }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] =
    ???

  // that match {
  //  case dataset: DatasetDataBag[A] => this.rep union dataset.rep
  //}

  override def distinct: DataBag[A] =
    ??? // rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def write[F <: io.Format : Meta](path: String, options: F#Config): Unit =
    ???

  override def write[F <: io.Format : Meta](path: String): Unit =
    ???

  def fetch(): Seq[A] =
    rep.collect()
}

object SparkDataset {

  import org.apache.spark.sql.Encoder

  implicit def encoderFor[T: Meta](implicit spark: SparkSession): Encoder[T] = {
    val tpe = implicitly[Meta[T]].ttag.tpe

    //@formatter:off
    if      ( tpe =:= typeOf[Byte]    ) spark.implicits.newByteEncoder.asInstanceOf[Encoder[T]]
    else if ( tpe =:= typeOf[Int]     ) spark.implicits.newIntEncoder.asInstanceOf[Encoder[T]]
    else if ( tpe =:= typeOf[Long]    ) spark.implicits.newLongEncoder.asInstanceOf[Encoder[T]]
    else if ( tpe =:= typeOf[Char]    ) spark.implicits.newShortEncoder.asInstanceOf[Encoder[T]]
    else if ( tpe =:= typeOf[Float]   ) spark.implicits.newFloatEncoder.asInstanceOf[Encoder[T]]
    else if ( tpe =:= typeOf[Double]  ) spark.implicits.newDoubleEncoder.asInstanceOf[Encoder[T]]
    //else if ( tpe <:< typeOf[Product] ) spark.implicits.newProductEncoder[T]
    else throw new RuntimeException("Unsupported")
    //@formatter:on
  }

  private implicit def wrap[A: Meta](rep: Dataset[A]): SparkDataset[A] =
    new SparkDataset(rep)

  def apply[A: Meta]()(implicit spark: SparkSession): SparkDataset[A] =
    spark.emptyDataset[A]

  def apply[A: Meta](seq: Seq[A])(implicit spark: SparkSession): SparkDataset[A] =
    spark.createDataset(seq)
}
