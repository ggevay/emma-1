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

import ast.AST
import org.emmalanguage.compiler.lang.AlphaEq
import org.emmalanguage.compiler.lang.cf.ControlFlow
import org.emmalanguage.compiler.lang.core.Core
import org.emmalanguage.compiler.lang.source.Source

import squid.ir.SimpleAST
import squid.ir.TopDownTransformer
import squid.quasi.MetaBases
import squid.quasi.ModularEmbedding

// !! You have to locally build Squid, see comment in the top-level pom.xml at <squid.version>

trait Squid extends AST with Common
  with AlphaEq with Source with ControlFlow
  with Core {

  import UniverseImplicits._

  object IR extends SimpleAST
  import IR.Predef._


  lazy val preSquid = TreeTransform("preSquid", Seq(
    Core.dscfInv,
    addImplicitPlaceholders,
    addValAndVarDefTpts,
    postProcess
  ))

  // TODO: maybe we should also call ANF here? Or the tree that Squid gives back is always in ANF (in our sense)?
  lazy val postSquid = TreeTransform("postSquid", Seq(
    TreeTransform("Compiler.typeCheck (after Squid)", (tree: u.Tree) => this.typeCheck(tree)),
    removeVarMutTpts,
    preProcess,
    Core.dscf, // FIXME: make sure to call this only if we were in DSCF before Squid
    changeToResolveNow
  ))


  // Squid expects tpt to be filled, but we keep it not filled for ValDefs
  // (because of https://github.com/emmalanguage/emma/issues/234)
  // So fill it now.
  lazy val addValAndVarDefTpts = TreeTransform("addValAndVarDefTpts", (t: u.Tree) => {
    api.TopDown.transform {
      case api.ValDef(lhs, rhs) => api.ValDef(lhs, rhs, true)
      case api.VarDef(lhs, rhs) => api.VarDef(lhs, rhs, true)
    }(t).tree
  })


  // I don't really understand why is this necessary, but here is what happens:
  // There is a transformation in DSCF that removes VarMuts, which causes the typeSafe assert in Transversers.scala
  // to fail (<notype> <!:< Unit), if the VarMut had a tpt (Unit).
  // What I don't understand is what is it in our pipeline that removes the tpts from VarMuts under normal
  // circumstances, i.e., why haven't we run into this problem before.
  lazy val removeVarMutTpts = TreeTransform("removeVarMutTpts", (t: u.Tree) => {
    api.TopDown.unsafe.transform {
      case api.VarMut(lhs, rhs) => api.VarMut(lhs, rhs) // VarMut.apply has setType(mut, Type.none) at the end
    }(t).tree
  })


  // See https://github.com/epfldata/squid/issues/55#issuecomment-395804672
  //
  // Note 1:
  // These placeholders need to be added right here. In particular, they cannot be added in preProcess,
  // because sometimes we change DefCalls in a way that the needed type of implicit arguments change: when the
  // backend transformations change from local DataBag calls to Flink/Spark calls, then the DefCalls suddenly need
  // the environment as an implicit.
  //
  // Note 2:
  // At first glance, it would seem to solve this whole problem if we would do `addContext` at the beginning of our
  // pipelines, because then we could resolve the real implicits here (instead of just placeholders).
  // However, this doesn't work, for two reasons:
  //  - If I put addContext at the beginning of our pipeline, I have a random test failure in BaseCodegenIntegrationSpec
  //    (even without any Squid stuff). This might be possible to fix, but see the next issue.
  //  - The real implicits sometimes resolve to anonymous classes (e.g., TypeInformationGen.mkCaseClassTypeInfo),
  //    which neither we, nor Squid don't support.
  lazy val addImplicitPlaceholders = TreeTransform("addImplicitPlaceholders", (t: u.Tree) => {
    api.TopDown.transform {
      case api.DefCall(target, method, targs, argss)
        if method.paramLists.size == argss.size + 1 && method != resolveLaterMirror && method != resolveNowMirror =>

        val placeholderArgList = method.paramLists.last.map { param =>
          api.DefCall(Some(api.Ref(implicitPlaceholdersModuleSym)), resolveLaterMirror,
            Seq(param.info.substituteTypes(method.typeParams, targs.toList)))
        }
        api.DefCall(target, method, targs, argss :+ placeholderArgList)
    }(t).tree
  })

  // FIXME: move this to postProcess (because like this there can't be more than one Squid transform in a pipeline)
  lazy val changeToResolveNow = TreeTransform("changeToResolveNow", (t: u.Tree) => {
    api.TopDown.transform {
      case api.DefCall(target, method, targs, argss)
        if method == resolveLaterMirror =>
        api.DefCall(target, resolveNowMirror, targs, argss)
    }(t).tree
  })

  lazy val implicitPlaceholdersModuleSym = api.Sym[org.emmalanguage.util.ImplicitPlaceholders.type].asModule
  lazy val resolveLaterMirror = implicitPlaceholdersModuleSym
    .info.member(api.TermName("resolveLater")).alternatives.collectFirst({
    case api.DefSym(m) => m
  }).get
  lazy val resolveNowMirror = implicitPlaceholdersModuleSym
    .info.member(api.TermName("resolveNow")).alternatives.collectFirst({
    case api.DefSym(m) => m
  }).get





  // TODO: Make this a parameter of testSquid
  object Tr extends squid.ir.SimpleRuleBasedTransformer with IR.SelfTransformer with TopDownTransformer {
    rewrite {
      case code"123" => code"42"
    }
  }





  lazy val testSquid = TreeTransform("Squid", (tree0: u.Tree) => {
    import u._

    val tree = preSquid(tree0)

    //TODO: remove these prints
    println("--- Giving Squid the following tree:\n" + showCode(tree))

    val thisNames = collection.mutable.Set[Name]()

    object ME extends ModularEmbedding[u.type, IR.type](u, IR,
      debug = str => println(str)) { // TODO: change 'debug' to avoid polluting compile-time stdout

      // See https://github.com/emmalanguage/emma/issues/369#issuecomment-397022718
      override def liftTerm(x: Tree, parent: Tree, expectedType: Option[Type], inVarargsPos: Boolean = false)
        (implicit ctx: Map[TermSymbol, IR.BoundVal]): IR.Rep = x match {
        case Select(This(typName),fieldName)
          if x.symbol != null
            && (x.symbol.isParameter || x.symbol.isMethod || x.symbol.isPrivateThis)
          // otherwise we capture things like `scala.collection.immutable`
          /* ^ we make a special case for `isPrivateThis` because it is the only case (I know of) where a class field
              will
              not have `isMethod` return true... that's still a heuristic (what if non-fields/parameters are
              PrivateThis?) */
          /*  The solution above (a case guarded by ad-hoc conditions) is not relly satisfying.
              We're going to capture method references that may have a static path to them and would be better with a
              static access.
              Ideally we'd check whether there is a static (and accessible from reflection) path first in the guard of
              the case.
              OTOH, does it really happen to have a static path to a local method, that will be accessible via the QQ's
              relfection?
              Note: `!x.symbol.isPackage` is not enough, as then we end up with things like the `List` of
              `scala.immutable.List` */
        =>
          //val thisName = s"$typName:this:$fieldName"
          val thisName = s"$typName.this.$fieldName"
          thisNames += TermName(thisName)
          base.hole(thisName, liftType(x.tpe))
        case _ => super.liftTerm(x,parent,expectedType,inVarargsPos)
      }

      override def unknownFeatureFallback(x: Tree, parent: Tree) = x match {

        case Ident(TermName(name)) =>
          base.hole(name, liftType(x.tpe))

        case _ =>
          super.unknownFeatureFallback(x, parent)

      }
    }

    val pgrm = IR.Code[Any, squid.utils.Bottom](ME(tree))

    val pgrm2 = pgrm transformWith Tr

    // putting things back into Scala (untyped trees):
    object MBM extends MetaBases {
      val u: Squid.this.u.type = Squid.this.u
      def freshName(hint: String) = api.TermName.fresh(hint)
    }
    val MB = new MBM.ScalaReflectionBase
    val res0 = IR.scalaTreeIn(MBM)(MB, pgrm2.rep, base.DefaultExtrudedHandler)

    println("--- Squid gave back:\n" + showCode(res0))

    // See https://github.com/emmalanguage/emma/issues/369#issuecomment-397022718
    // Do this before typecheck!
    val res00 = api.TopDown.transform {
      case Ident(name) if thisNames(name) =>
        //val Seq(typ,ths,field) = name.toString.splitSane(':')
        import squid.utils.StringOps
        val Seq(typ,ths,field) = name.toString.splitSane('.')
        Select(This(TypeName(typ)),TermName(field))
    }(res0).tree

    val res = postSquid(res00)
    println("--- After postSquid:\n" + showCode(res))
    res
  })

}
