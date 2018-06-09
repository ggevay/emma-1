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

import squid.ir.SimpleAST
import squid.ir.TopDownTransformer
import squid.quasi.MetaBases
import squid.quasi.ModularEmbedding

// !! You have to locally build Squid, see comment in the top-level pom.xml at <squid.version>

trait Squid extends AST {

  import UniverseImplicits._

  object IR extends SimpleAST
  import IR.Predef._




  lazy val implicitPlaceholdersModuleSym = api.Sym[org.emmalanguage.util.ImplicitPlaceholders.type].asModule
  lazy val resolveLaterMirror = implicitPlaceholdersModuleSym
    .info.member(api.TermName("resolveLater")).alternatives.collectFirst({
    case api.DefSym(m) => m
  }).get
  lazy val resolveNowMirror = implicitPlaceholdersModuleSym
    .info.member(api.TermName("resolveNow")).alternatives.collectFirst({
    case api.DefSym(m) => m
  }).get







  lazy val preSquid = TreeTransform("preSquid", Seq(
    addValDefTpt
    ,addImplicitPlaceholders
  ))


  // Squid expects tpt to be filled, but we keep it not filled for ValDefs
  // (because of https://github.com/emmalanguage/emma/issues/234)
  // So fill it now.
  lazy val addValDefTpt = TreeTransform("AddValDefTpt", (t: u.Tree) => {
    api.TopDown.transform {
      case api.ValDef(lhs, rhs) => api.ValDef(lhs, rhs, true)
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
        if method.name.toString.contains("resolveLater") =>
        api.DefCall(target, resolveNowMirror, targs, argss)
    }(t).tree
  })




  // TODO: Make this a parameter of testSquid
  object Tr extends squid.ir.SimpleRuleBasedTransformer with IR.SelfTransformer with TopDownTransformer {
    rewrite {
      case code"123" => code"42"
    }
  }





  lazy val testSquid = TreeTransform("Squid", (tree0: u.Tree) => {
    import u._

    val tree = preSquid(tree0)

    object ME extends ModularEmbedding[u.type, IR.type](u, IR,
      debug = str => println(str)) // change 'debug' to avoid polluting compile-time stdout
    val pgrm = IR.Code[Any, squid.utils.Bottom](ME(tree))

    //TODO: remove these prints

    println(s"FIRST >> ${pgrm}")
    val pgrm2 = pgrm transformWith Tr

    println(s"SECOND >> ${pgrm2}")
    //val msg = s"[At compile time:]\n\tOriginal program: $pgrm\n\tTransformed program: $pgrm2"

    // putting things back into Scala (untyped trees):
    object MBM extends MetaBases {
      val u: Squid.this.u.type = Squid.this.u
      def freshName(hint: String) = api.TermName.fresh(hint) //u.freshTermName(TermName(hint))
    }
    val MB = new MBM.ScalaReflectionBase
    val res = IR.scalaTreeIn(MBM)(MB, pgrm2.rep, base.DefaultExtrudedHandler)

    println("Generated Scala tree: " + showCode(res))

    //q"println($msg); $res"


    // TODO: We should actually call our whole preprocess pipeline after typechecking (it's not too much time)
    //
    // Note: If you remove the typecheck, you can at least test whether Squid can get the trees we give to it
    // into its IR. And actually, there are several random issues with trees in BaseCodegenIntegrationSpec.
    changeToResolveNow(typeCheck(res))
  })

}
