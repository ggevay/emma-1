package org.emmalanguage

import eu.stratosphere.emma.macros.Folds

import scala.reflect.{classTag, ClassTag}
import scala.reflect.runtime.universe._

/** Package object for the Emma API. Contains default methods and definitions. */
package object api {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // types supported by Emma
  // -----------------------------------------------------

  trait Meta[T] {
    type Type = T
    val ctag: ClassTag[T]
    val ttag: TypeTag[T]
  }

  // primitive types

  implicit object ByteMeta extends Meta[Byte] {
    override val ctag = classTag[Byte]
    override val ttag = typeTag[Byte]
  }

  implicit object IntMeta extends Meta[Int] {
    override val ctag = classTag[Int]
    override val ttag = typeTag[Int]
  }

  implicit object LongMeta extends Meta[Long] {
    override val ctag = classTag[Long]
    override val ttag = typeTag[Long]
  }

  implicit object CharMeta extends Meta[Char] {
    override val ctag = classTag[Char]
    override val ttag = typeTag[Char]
  }

  implicit object FloatMeta extends Meta[Float] {
    override val ctag = classTag[Float]
    override val ttag = typeTag[Float]
  }

  implicit object DoubleMeta extends Meta[Double] {
    override val ctag = classTag[Double]
    override val ttag = typeTag[Double]
  }

  // product types

  class ProductMeta[T <: Product : ClassTag : TypeTag] extends Meta[T] {
    override val ctag = implicitly[ClassTag[T]]
    override val ttag = implicitly[TypeTag[T]]
  }

  implicit def productType[T <: Product : ClassTag : TypeTag] = new ProductMeta[T]

  // implicit type metainformation projections

  implicit def ttagForType[T: Meta]: TypeTag[T] =
    implicitly[Meta[T]].ttag

  implicit def ctagForType[T: Meta]: ClassTag[T] =
    implicitly[Meta[T]].ctag

  // -----------------------------------------------------
  // limits
  // -----------------------------------------------------

  trait Limits[T] {
    val min: T
    val max: T
  }

  implicit val ByteLimits = new Limits[Byte] {
    val min = Byte.MinValue
    val max = Byte.MaxValue
  }

  implicit val IntLimits = new Limits[Int] {
    val min = Int.MinValue
    val max = Int.MaxValue
  }

  implicit val LongLimits = new Limits[Long] {
    val min = Long.MinValue
    val max = Long.MaxValue
  }

  implicit val CharLimits = new Limits[Char] {
    val min = Char.MinValue
    val max = Char.MaxValue
  }

  implicit val FloatLimits = new Limits[Float] {
    val min = Float.MinValue
    val max = Float.MaxValue
  }

  implicit val DoubleLimits = new Limits[Double] {
    val min = Double.MinValue
    val max = Double.MaxValue
  }

  // -----------------------------------------------------
  // Converters
  // -----------------------------------------------------

  /**
   * Extend the [[DataBag]] type with methods from [[Folds]] via the "pimp my library" pattern.
   * This is a value class, which means that in most cases, the allocation of an instance can be
   * avoided when using the defined methods.
   *
   * @param self the actual [[DataBag]] instance
   * @tparam A the type of elements to fold over
   */
  implicit final class DataBagFolds[A] private[api](val self: DataBag[A])
    extends AnyVal with Folds[A]

  def comparing[A](lt: (A, A) => Boolean): Ordering[A] =
    Ordering.fromLessThan(lt)
}
