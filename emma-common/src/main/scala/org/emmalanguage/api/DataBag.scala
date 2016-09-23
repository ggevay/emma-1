package org.emmalanguage.api

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
 * An abstraction for homogeneous collections.
 */
trait DataBag[A] extends Serializable {

  type Self[X] <: DataBag[X]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  /**
   * Structural recursion over the bag.
   * Assumes an algebraic specification of the DataBag type using three constructors:
   *
   * {{{
   * sealed trait DataBag[A]
   * case class Sng[A](x: A) extends DataBag[A]
   * case class Union[A](xs: DataBag[A], ys: Bag[A]) extends DataBag[A]
   * case object Empty extends DataBag[Nothing]
   * }}}
   *
   * The specification of this function can be therefore interpreted as the following program:
   *
   * {{{
   * this match {
   *   case Empty => z
   *   case Sng(x) => s(x)
   *   case Union(xs, ys) => p(xs.fold(z)(s, u), ys.fold(z)(s, u))
   * }
   * }}}
   *
   * @param z Substitute for Empty
   * @param s Substitute for Sng
   * @param u Substitute for Union
   * @tparam B The result type of the recursive computation
   * @return
   */
  def fold[B: ClassTag](z: B)(s: A => B, u: (B, B) => B): B

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  /**
   * Monad map.
   *
   * @param f Function to be applied on the collection.
   * @tparam B Type of the output DataBag.
   * @return A DataBag containing the elements `f(x)` produced for each element x of the input.
   */
  def map[B: ClassTag](f: (A) => B): Self[B]

  /**
   * Monad flatMap.
   *
   * @param f Function to be applied on the collection. The resulting bags are flattened.
   * @tparam B Type of the output DataBag.
   * @return A DataBag containing the union (flattening) of the DataBags `f(x)` produced for each element of the input.
   */
  def flatMap[B: ClassTag](f: (A) => Self[B]): Self[B]

  /**
   * Monad filter.
   *
   * @param p Predicate to be applied on the collection. Only qualifying elements are passed down the chain.
   * @return
   */
  def withFilter(p: (A) => Boolean): Self[A]

  // -----------------------------------------------------
  // Grouping and Set operations
  // -----------------------------------------------------

  /**
   * Groups the bag by key.
   *
   * @param k Key selector function.
   * @tparam K Key type.
   * @return A version of this bag with the entries grouped by key.
   */
  def groupBy[K: ClassTag](k: (A) => K): DataBag[Group[K, DataBag[A]]]

  /**
   * Plus operator (union). Respects duplicates, e.g.:
   *
   * {{{
   * DataBag(Seq(1,1,2,3)) plus DataBag(Seq(1,2,5)) = DataBag(Seq(1,1,2,3,1,2,5))
   * }}}
   *
   * @param addend The second addend parameter.
   * @return The set-theoretic union (with duplicates) between this DataBag and the given subtrahend.
   */
  def plus(addend: Self[A]): DataBag[A]

  /**
   * Removes duplicate entries from the bag, e.g.
   *
   * {{{
   * DataBag(Seq(1,1,2,3)).distinct() = DataBag(Seq(1,2,3))
   * }}}
   *
   * @return A version of this DataBag without duplicate entries.
   */
  def distinct(): DataBag[A]

  // -----------------------------------------------------
  // Conversion Methods
  // -----------------------------------------------------
  /**
   * Converts a DataBag abstraction back into a scala sequence.
   *
   * @return The contents of the DataBag as a scala sequence.
   */
  def fetch(): Seq[A]
}

object DataBag {

  /**
   * Empty constructor.
   *
   * @tparam A The element type for the DataBag.
   * @return An empty DataBag for elements of type A.
   */
  def apply[A: ClassTag](): DataBag[A] = new SeqDataBag(Seq.empty)

  /**
   * Sequence constructor.
   *
   * @param values The values contained in the bag.
   * @tparam A The element type for the DataBag.
   * @return A DataBag containing the elements of the `values` sequence.
   */
  def apply[A: ClassTag](values: Seq[A]): DataBag[A] = new SeqDataBag(values)
}