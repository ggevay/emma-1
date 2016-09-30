package eu.stratosphere.emma.api

import eu.stratosphere.emma.macros.program.WorkflowMacros
import eu.stratosphere.emma.macros.utility.UtilMacros
import eu.stratosphere.emma.macros.program.Parallelize2

import scala.language.experimental.macros

// TODO: Add more detailed documentation with examples.
object emma {

  // -----------------------------------------------------
  // program macros
  // -----------------------------------------------------

  final def parallelize[T](e: T): Algorithm[T] =
    macro WorkflowMacros.parallelize[T]

  final def parallelize2[T](e: T): Algorithm[T] =
    macro Parallelize2.parallelize[T]

  final def comprehend[T](e: T): Unit =
    macro WorkflowMacros.comprehend[T]

  final def visualize[T](e: T): T =
    macro UtilMacros.visualize[T]

  final def prettyPrint[T](e: T): String =
    macro UtilMacros.prettyPrint[T]
}
