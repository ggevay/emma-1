package eu.stratosphere.emma
package compiler

import ast.JavaAST

import java.net.URLClassLoader
import java.nio.file.{Files, Paths}

import scala.tools.reflect.ToolBoxFactory

/** A reflection-based [[Compiler]]. */
class RuntimeCompiler extends Compiler with JavaAST {

  import RuntimeCompiler._
  import universe._

  /** The directory where the toolbox will store runtime-generated code. */
  val codeGenDir = {
    val path = Paths.get(sys.props.getOrElse("emma.codegen.dir", default.codeGenDir))
    // Make sure that generated class directory exists
    Files.createDirectories(path)
    path.toAbsolutePath.toString
  }

  /** The generating Scala toolbox. */
  override val tb = {
    val cl = getClass.getClassLoader
    val factory = new ToolBoxFactory[universe.type](universe) {
      val mirror = runtimeMirror(cl)
    }

    cl match {
      case urlCL: URLClassLoader =>
        // Append current classpath to the toolbox in case of a URL classloader
        // (fixes a bug in Spark)
        val cp = urlCL.getURLs.map(_.getFile).mkString(sys.props("path.separator"))
        factory.mkToolBox(options = s"-d $codeGenDir -cp $cp")
      case _ =>
        // Use toolbox without extra classpath entries otherwise
        factory.mkToolBox(options = s"-d $codeGenDir")
    }
  }

  def specPipeline(typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true)
    (transformations: (u.Tree => u.Tree)*): u.Tree => u.Tree = {

    pipeline(typeCheck, withPre, withPost)(transformations: _*) andThen {(tree: u.Tree) =>
      tb.compile(tb.parse(u.showCode(tree)))
      tree
    }
  }
}

object RuntimeCompiler {

  /** [[RuntimeCompiler]] defaults. */
  object default {

    val runMethod = "run"

    lazy val codeGenDir = Paths
      .get(sys.props("java.io.tmpdir"), "emma", "codegen")
      .toAbsolutePath.toString
  }
}
