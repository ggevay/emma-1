package org.emmalanguage
package api

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


class ScalaTraversableSpec extends DataBagSpec {

  override type Bag[A] = ScalaTraversable[A]
  override type BackendContext = Unit

  override def withBackendContext[T](f: BackendContext => T): T =
    f(Unit)

  override def Bag[A: Meta : ClassTag : TypeTag](implicit sc: BackendContext): Bag[A] =
    ScalaTraversable[A]

  override def Bag[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit sc: BackendContext): Bag[A] =
    ScalaTraversable(seq)
}
