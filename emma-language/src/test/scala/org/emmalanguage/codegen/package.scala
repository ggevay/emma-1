/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage

import org.emmalanguage.test.schema.Graphs.LabelledEdge
import eu.stratosphere.emma.api.model.Identity

package object codegen {
  // schema
  case class State(identity: Long, var value: Int) extends Identity[Long]
  case class Update(identity: Long, inc: Int) extends Identity[Long]

  // predicates
  def p1(x: Int) = x < 2
  def p2(x: Int) = x > 5
  def p3(x: Int) = x == 0

  // values
  val graph = Seq(
    LabelledEdge(1, 4, "A"),
    LabelledEdge(2, 5, "B"),
    LabelledEdge(3, 6, "C"))
}
