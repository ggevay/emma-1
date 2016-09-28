package org.emmalanguage
package api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.TypeUtils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta : ClassTag : TypeTag] private[api](private val rep: DataSet[A]) extends DataBag[A] {

  override type TypeClass[T] = TypeInformation[T]

  import FlinkDataSet.{wrap}

  import org.apache.flink.api.scala._

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta : ClassTag : TypeTag: WeakTypeTag: TypeClass](z: B)(s: A => B, u: (B, B) => B): B = {

    //
//    import scala.reflect.runtime.universe._
//    val ctag = implicitly[ClassTag[B]]
//    val ttag = implicitly[TypeTag[B]]
    val typeInfo = implicitly[TypeInformation[B]]
    //

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

  def write[F <: io.Format : Meta : ClassTag : TypeTag](path: String, options: F#Config): Unit =
    ???

  override def write[F <: io.Format : Meta : ClassTag : TypeTag](path: String): Unit =
    ???

  def fetch(): Seq[A] =
    rep.collect()
}

object FlinkDataSet {

  import org.apache.flink.api.common.typeinfo.TypeInformation



//  implicit def typeInformationForType[T: Meta : ClassTag : TypeTag]: TypeInformation[T] = {
//    implicit def createTypeInformationLocal[T]: TypeInformation[T] = org.apache.flink.api.scala.createTypeInformation[T]
//    //org.apache.flink.api.scala.createTypeInformation[T]
//
//
//
//    createTypeInformationLocal[T]
//  }

  implicit def wrap[A: Meta : ClassTag : TypeTag](rep: DataSet[A]): FlinkDataSet[A] =
    new FlinkDataSet(rep)

  def apply[A: Meta : ClassTag : TypeTag]()(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromElements[A]()

  def apply[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromCollection(seq)
}