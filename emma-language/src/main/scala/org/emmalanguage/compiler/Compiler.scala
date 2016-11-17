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
package compiler

import lang.AlphaEq
import lang.backend.Backend
import lang.cf.ControlFlow
import lang.core.Core
import lang.libsupport.LibSupport
import lang.source.Source

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends AlphaEq
  with LibSupport
  with Source
  with Core
  with Backend
  with ControlFlow {

  /** The underlying universe object. */
  override val universe: Universe

  /** Standard pipeline prefix. Brings a tree into a form convenient for transformation. */
  lazy val preProcess: Seq[u.Tree => u.Tree] = Seq(
    Source.removeImplicits(API.implicitTypes),
    fixSymbolTypes,
    stubTypeTrees,
    unQualifyStatics,
    normalizeStatements,
    Source.normalize
  )

  /** Standard pipeline suffix. Brings a tree into a form acceptable for `scalac` after being transformed. */
  lazy val postProcess: Seq[u.Tree => u.Tree] = Seq(
    api.Owner.at(get.enclosingOwner),
    qualifyStatics,
    restoreTypeTrees
  )

  /** The identity transformation with pre- and post-processing. */
  def identity(typeCheck: Boolean = false): u.Tree => u.Tree =
    pipeline(typeCheck)()

  /** Combines a sequence of `transformations` into a pipeline with pre- and post-processing. */
  def pipeline(typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true)
    (transformations: (u.Tree => u.Tree)*): u.Tree => u.Tree = {

    val bld = Seq.newBuilder[u.Tree => u.Tree]
    //@formatter:off
    if (typeCheck) bld += { api.Type.check(_) }
    if (withPre)   bld ++= preProcess
    bld ++= transformations
    if (withPost)  bld ++= postProcess
    //@formatter:on
    val steps = bld.result()

    if (!printAllTrees) Function.chain(steps)
    else Function.chain(List(printPipelineStart, printTree) ++ steps.flatMap(List(_, printTree)))
  }

  // -----------------------------------------------------
  // Debugging
  // -----------------------------------------------------

  // Turn this on to print the tree between every step in the pipeline (also before the first and after the last steps).
  private val printAllTrees = true

  private lazy val printPipelineStart: u.Tree => u.Tree = {
    (tree: u.Tree) => {
      println("################## Starting compilation pipeline ##################")
      tree
    }
  }

  private lazy val printTree: u.Tree => u.Tree = {
    (tree: u.Tree) => {
      println("=============================")
      println(u.showCode(tree))
      println("=============================")
      tree
    }
  }
}
