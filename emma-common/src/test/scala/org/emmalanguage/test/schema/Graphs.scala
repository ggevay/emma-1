package org.emmalanguage.test.schema

object Graphs {
  case class LabelledEdge[V, L](src: V, dst: V, label: L)
  case class Edge[V](src: V, dst: V)
}
