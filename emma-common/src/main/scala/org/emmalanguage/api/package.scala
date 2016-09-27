package org.emmalanguage

import eu.stratosphere.emma.macros.Folds

/** Package object for the Emma API. Contains default methods and definitions. */
package object api {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // limits
  // -----------------------------------------------------

  trait Limits[T] {
    val min: T
    val max: T
  }

  implicit object ByteLimits extends Limits[Byte] {
    val min = Byte.MinValue
    val max = Byte.MaxValue
  }

  implicit object IntLimits extends Limits[Int] {
    val min = Int.MinValue
    val max = Int.MaxValue
  }

  implicit object LongLimits extends Limits[Long] {
    val min = Long.MinValue
    val max = Long.MaxValue
  }

  implicit object CharLimits extends Limits[Char] {
    val min = Char.MinValue
    val max = Char.MaxValue
  }

  implicit object FloatLimits extends Limits[Float] {
    val min = Float.MinValue
    val max = Float.MaxValue
  }

  implicit object DoubleLimits extends Limits[Double] {
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
