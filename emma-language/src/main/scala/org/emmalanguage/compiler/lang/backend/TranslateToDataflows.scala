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
package compiler.lang.backend

import cats.std.all._
import compiler.Common
import compiler.lang.core.Core
import util.Monoids

import shapeless._

/** Translating DataBag programs to dataflow APIs, like Flink DataSets or Spark RDDs. */
private[backend] trait TranslateToDataflows extends Common {
  self: Backend with Core =>

  import UniverseImplicits._
  import Core.{Lang => core}
  
  private[backend] object TranslateToDataflows {

    /**
     * Translates into a dataflow on the given backend.
     *
     * == Preconditions ==
     *
     * - The input tree is in Core, but without comprehensions.
     *
     * == Postconditions ==
     *
     * - A tree where DataBag operations have been translated to dataflow operations.
     *
     * @param backendSymbol The symbol of the target backend.
     */
    def translateToDataflows(backendSymbol: u.ModuleSymbol): u.Tree => u.Tree = tree => {

      // Figure out the order stuff
      val (disambiguatedTree, highFuns): (u.Tree, Set[u.TermSymbol]) = Order.disambiguate(tree)

      val withHighContext = api.TopDown.inherit {
        case core.ValDef(sym, _, _) if highFuns contains sym => true
      }(Monoids.disj)

      // Change static method calls to their corresponding backend calls.
      // The backend methods should be defined in the module `backendSymbol`,
      // and they should not have any overloads (this is because we look up the corresponding method
      // just by name; adding overload resolution would be possible but complicated because of the additional
      // implicit parameters for the backend contexts).
      val moduleSymbols = Set(API.bagModuleSymbol, ComprehensionCombinators.module)
      val methods = API.sourceOps ++ ComprehensionCombinators.ops
      val staticCallsChanged = withHighContext.transformWith {
        case Attr.inh(core.DefCall(Some(api.ModuleRef(moduleSymbol)), method, targs, argss@_*), false :: _)
          if moduleSymbols(moduleSymbol) && methods(method) => {
          val targetMethodDecl = backendSymbol.info.decl(method.name)
          assert(targetMethodDecl.alternatives.size == 1,
            s"Target method `${method.name}` (found as `$targetMethodDecl`) should have exactly one overload.")
          val targetMethod = targetMethodDecl.asMethod
          core.DefCall(Some(api.ModuleRef(backendSymbol)))(targetMethod, targs: _*)(argss: _*)
        }
      }(disambiguatedTree).tree

      // Gather DataBag vals that are referenced from higher-order context
      val Attr.acc(_, valsRefdFromHigh :: _) =
        withHighContext.accumulateWith[Set[u.TermSymbol]] {
          case Attr.inh(core.ValRef(sym), true :: _)
          if api.Type.of(sym).typeConstructor =:= API.DataBag =>
            Set(sym)
        }
        .traverseAny(staticCallsChanged)

      // Insert fetch calls for the vals in valsRefdFromHigh (but only if they are defined at first-order)
      /*
        val $lhs = $rhs
        ==>
        val $lhs = {
          val orig = $rhs
          val fetched = ScalaSeq.byFetch(orig)
          fetched
        }
       */
      val fetchesInserted =
        withHighContext.transformWith {
          case Attr.inh(api.ValDef(lhs, rhs, flags), false :: _)
          if valsRefdFromHigh(lhs) =>
            val origSym = api.TermSym(lhs, api.TermName.fresh("orig"), api.Type.of(lhs)) // will be the original bag
            val fetchCall = core.DefCall(Some(api.ModuleRef(API.scalaSeqModuleSymbol))) (
              API.byFetch,
              Core.bagElemTpe(core.ValRef(lhs))) (
              Seq(core.ValRef(origSym)))
            val fetchedSym = api.TermSym(lhs, api.TermName.fresh("fetched"), api.Type.of(fetchCall))
            val fetchedVal = core.ValDef(fetchedSym, fetchCall, flags)
            core.ValDef(
              lhs,
              core.Let(api.ValDef(origSym, rhs), fetchedVal)()(core.ValRef(fetchedSym)),
              flags) // Note: the flags are present also here, and on fetchedVal.
        }(staticCallsChanged).tree

      // Gather symbols that should be persisted. These are:
      //  TODO
      val Attr.acc(_, toPersist :: _) =
        api.TopDown.withValUses
        // Innermost enclosing loop
        .inherit {
          case core.DefDef(method,_,_,_,_) if method.name.toString.matches("""(while|doWhile)\$.*""") =>
            method
        }(Monoids.right(null))
        // Map from ValDefs to their innermost enclosing loops
        .accumulateWith[Map[u.TermSymbol, u.MethodSymbol]] {
          case Attr.inh(core.ValDef(sym, _, _), enclLoop :: _)
            if api.Type.of(sym).typeConstructor =:= API.DataBag =>
            Map(sym -> enclLoop)
        }(Monoids.overwrite)
        // Symbols which should be persisted
        .accumulateWith[Set[u.TermSymbol]] {
          case Attr.all(api.ValRef(sym), _ :: valEnclLoops :: _, enclLoop :: _, valUses :: _)
            if api.Type.of(sym).typeConstructor =:= API.DataBag &&
              (valUses(sym) > 1 || enclLoop != valEnclLoops(sym)) =>
            Set(sym)
          case Attr.none(api.DefCall(_, method, _, argss@_*))
            if method.name.toString.matches("""(while|doWhile)\$.*""") =>
            argss.flatMap {x => x.flatMap {
              case api.ValRef(sym)
                if api.Type.of(sym).typeConstructor =:= API.DataBag => Seq(sym)
              case _ => Seq()
            }}.toSet
        }
        .traverseAny(fetchesInserted)

      // Insert persist calls
      /*
      val $lhs = $rhs
      ==>
      val $lhs = {
        val orig = $rhs
        val persisted = orig.persist
        persisted
      }
       */
      val persistsInserted =
        withHighContext.transformWith {
          case Attr.inh(api.ValDef(lhs, rhs, flags), false :: _)
            if toPersist(lhs) =>
              val origSym = api.TermSym(lhs, api.TermName.fresh("orig"), api.Type.of(lhs)) // will be the original bag
              val persistCall = core.DefCall(Some(core.ValRef(origSym)))(API.persist)()
              val persistedSym = api.TermSym(lhs, api.TermName.fresh("persisted"), api.Type.of(persistCall))
              val persistedVal = core.ValDef(persistedSym, persistCall, flags)
              core.ValDef(
                lhs,
                core.Let(api.ValDef(origSym, rhs), persistedVal)()(core.ValRef(persistedSym)),
                flags) // Note: the flags are present also here, and on persistedVal.
        }(fetchesInserted).tree

      // The flatten is needed because we put a Lets on the rhs of ValDefs when inserting collects and persists
      Core.flatten(persistsInserted)
    }

  }
}
