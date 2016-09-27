package org.emmalanguage
package api.io

abstract class Support[F <: Format] {

  private[api] def read[A](path: String): TraversableOnce[A]

  private[api] def write[A](path: String)(xs: Traversable[A]): Unit
}
