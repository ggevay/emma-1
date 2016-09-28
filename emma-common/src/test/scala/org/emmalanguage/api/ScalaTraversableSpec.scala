package org.emmalanguage
package api

class ScalaTraversableSpec extends DataBagSpec {

  override type Bag[A] = ScalaTraversable[A]
  override type BackendContext = Unit

  override def withBackendContext[T](f: BackendContext => T): T =
    f(Unit)

  override def Bag[A: Meta](implicit sc: BackendContext): Bag[A] =
    ScalaTraversable[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit sc: BackendContext): Bag[A] =
    ScalaTraversable(seq)
}
