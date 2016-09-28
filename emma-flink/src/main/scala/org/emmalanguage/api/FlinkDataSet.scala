package org.emmalanguage
package api

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api](private val rep: DataSet[A]) extends DataBag[A] {

  import FlinkDataSet.{wrap, typeInformationForType}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B = {
    val collected = rep.map(x => s(x)).reduce(u).collect()
    if (collected.isEmpty) {
      z
    } else {
      assert(collected.size == 1)
      collected.head
    }
  }

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
    rep.groupBy(k).reduceGroup((it: Iterator[A]) => {
      val buffer = it.toBuffer // This is because the iterator might not be serializable
      Group(k(buffer.head), DataBag(buffer))
    })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dataset: FlinkDataSet[A] => this.rep union dataset.rep
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
}

object FlinkDataSet {

  import org.apache.flink.api.common.typeinfo.TypeInformation

  implicit def typeInformationForType[T: Meta]: TypeInformation[T] =
    org.apache.flink.api.scala.createTypeInformation

  implicit def wrap[A: Meta](rep: DataSet[A]): FlinkDataSet[A] =
    new FlinkDataSet(rep)

  def apply[A: Meta]()(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromElements[A]()

  def apply[A: Meta](seq: Seq[A])(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromCollection(seq)
}