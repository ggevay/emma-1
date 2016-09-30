package eu.stratosphere.emma
package macros
package program

import compiler.MacroCompiler
import api.Algorithm
import runtime.{Engine, Native}

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox




class Parallelize2(val c: blackbox.Context) extends MacroCompiler {

  import universe._

  val ENGINE = typeOf[Engine]
  val NATIVE = typeOf[Native]

  /** Translate an Emma expression to an [[Algorithm]]. */
  // TODO: Add more comprehensive ScalaDoc
  def parallelize[T: c.WeakTypeTag](e: Expr[T]) = {

    val tree = api.Type.unCheck(e.tree)

    // Construct algorithm object
    api.Type.check(q"""new _root_.eu.stratosphere.emma.api.Algorithm[${weakTypeOf[T]}] with Serializable {
      import _root_.scala.reflect._

      def run(engine: $ENGINE): ${weakTypeOf[T]} = engine match {
        case _: $NATIVE => runNative()
        case _ => runParallel(engine)
      }

      private def runNative(): ${weakTypeOf[T]} = {
        ${api.Type.unCheck(e.tree)}
      }

      private def runParallel(engine: $ENGINE): ${weakTypeOf[T]} = {
        val compiler = new _root_.eu.stratosphere.emma.compiler.RuntimeCompiler()

        //reify{()}.tree
        //q"()"

        val t = $tree
        //q"$$t"
        //compiler.tb.eval(q"$$t")

        compiler.tb.eval(compiler.tb.parse(${api.Tree.show(tree)})).asInstanceOf[${weakTypeOf[T]}]


      }
    }""")

    // compiler.tb.eval(reify{$tree}.tree)

//    val t = $tree
//    compiler.tb.eval(q"$$t")

  }

}
