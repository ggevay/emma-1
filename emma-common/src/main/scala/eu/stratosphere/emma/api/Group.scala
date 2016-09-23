package eu.stratosphere.emma
package api

import model.Identity

case class Group[K, +V](key: K, values: V) extends Identity[K] {
  def identity = key
}
