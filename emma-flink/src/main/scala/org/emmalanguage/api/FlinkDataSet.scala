package org.emmalanguage
package api

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.language.{higherKinds, implicitConversions}

import java.io.{ObjectInputStream, ObjectOutputStream}

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api](private val rep: DataSet[A]) extends DataBag[A] {

  import FlinkDataSet.{wrap, typeInfoForType}

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

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] = {
    rep.groupBy(k).reduceGroup((it: Iterator[A]) => {
      val buffer = it.toBuffer // This is because the iterator might not be serializable
      Group(k(buffer.head), DataBag(buffer))
    })
  }

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
  import scala.reflect.runtime.currentMirror
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  import java.io.IOException

  private lazy val flinkApi = currentMirror.staticModule("org.apache.flink.api.scala.package")
  private lazy val typeInfo = flinkApi.info.decl(TermName("createTypeInformation"))
  private lazy val toolbox = currentMirror.mkToolBox()

  class SynthesizedTypeInfo[T: TypeTag] extends TypeInformation[T] {

    val tpe = implicitly[TypeTag[T]].tpe

    @transient var imp: TypeInformation[T] = toolbox.eval(q"$typeInfo[$tpe]").asInstanceOf[TypeInformation[T]]

    override def isBasicType: Boolean =
      imp.isBasicType

    override def canEqual(obj: scala.Any): Boolean =
      imp.canEqual(obj)

    override def getTotalFields: Int =
      imp.getTotalFields

    override def createSerializer(config: ExecutionConfig): TypeSerializer[T] =
      imp.createSerializer(config)

    override def getArity: Int =
      imp.getArity

    override def isKeyType: Boolean =
      imp.isKeyType

    override def getTypeClass: Class[T] =
      imp.getTypeClass

    override def isTupleType: Boolean =
      imp.isTupleType

    override def toString: String =
      imp.toString

    override def equals(obj: Any): Boolean =
      imp equals obj

    override def hashCode(): Int =
      imp.hashCode()

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {
      in.defaultReadObject()
      imp = toolbox.eval(q"$typeInfo[$tpe]").asInstanceOf[TypeInformation[T]]
    }
  }

  implicit def typeInfoForType[T: Meta]: TypeInformation[T] =
    new SynthesizedTypeInfo[T]

  implicit def wrap[A: Meta](rep: DataSet[A]): FlinkDataSet[A] =
    new FlinkDataSet(rep)

  def apply[A: Meta]()(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromElements[A]()

  def apply[A: Meta](seq: Seq[A])(implicit flink: ExecutionEnvironment): FlinkDataSet[A] =
    flink.fromCollection(seq)
}