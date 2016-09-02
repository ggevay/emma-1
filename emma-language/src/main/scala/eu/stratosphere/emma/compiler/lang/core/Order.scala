package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common
import eu.stratosphere.emma.util.Monoids
import cats.std.all._
import shapeless._

/**
 * Determining which parts of the code might be called from a higher-order context, that is from a UDF of a combinator.
 * Note that some code might be executed both at the driver and in UDFs, which need to be disambiguated, because
 * we will later transform higher-order code differently than first-order code.
 *
 * Note: This code assumes that DataBags don't contain functions. (E.g. it handles a DataBag[Int => Int] incorrectly.)
 */
private[core] trait Order extends Common {
  self: Core =>

  import Core.{Lang => core}
  import UniverseImplicits._

  private[core] object Order {

    /**
     * If a lambda is given as an argument to one of these methods,
     * then that lambda will be called from higher-order context.
     */
    val combinators = API.methods ++ ComprehensionCombinators.methods

    /**
     * Disambiguates order in a tree and gives information on which parts of the code might be executed
     * from a higher-order context.
     *
     * == Preconditions ==
     *
     * - Input must be in ANF.
     * - DataBags don't contain functions. (TODO: Maybe add a check for this in some place like CoreValidate)
     *
     * == Postconditions ==
     *
     * Returns a (disambiguatedTree, isHighContext) pair, where
     * disambiguatedTree is an ANF tree where it has been decided for every piece of code whether to treat it
     * as driver-only or high (that is, it can be called from a higher-order context). (Some ValDefs have been
     * duplicated for this.)
     *
     * isHighContext can be used as an inherited attribute to know which code might be called from higher-order context
     * (e.g. like this: api.BottomUp.inherit(isHighContext)(Monoids.disj).transform{...}(disambiguatedTree)).
     * Note, that isHighContext might contain false positives, i.e. it might give true for code that is only executed
     * in the driver.
     *
     * Warning: isHighContext depends on information about all the lambdas in disambiguatedTree and looking at the
     * combinator calls. Therefore, it will not work on a tree which was got from disambiguatedTree by changing
     * the lambdas or the combinator calls. (Note: this is why the backend transformations have to go BottomUp.)
     *
     * Algorithm:
     * 1. Collect symbols of all function ValDefs. (funs)
     * 2. Build a graph of funs referencing funs. (funGraph)
     * 3. Collect funs that are given as arguments to combinators. (topLevRefs)
     * 4. Collect funs referenced from top-level. (combRefs)
     * 5. Do graph traversals on funGraph from combRefs and topLevRefs to get all high and
     *    driver-only funs, respectively. (highFuns0, driverFuns)
     * 6. The intersection of highFuns0 and driverFuns are the ambiguous ones. Create new ValDefs
     *    for these, and modify refs to them in high code to the newly created vals.
     */
    def disambiguate(tree: u.Tree): (u.Tree, u.Tree =?> Boolean) = {

      def isFun(sym: u.TermSymbol) = api.Sym.funs(api.Type.of(sym).typeSymbol)

      // The Set of symbols of ValDefs of functions
      val funs = tree.collect {
        case api.ValDef(sym, _, _) if isFun(sym) => sym
      }.toSet

      val Attr.all(_, topLevRefs :: combRefs :: _, _, (_, funGraph) :: _) =
        api.TopDown
        // funGraph (the second element of the tuple is the graph, the first is just auxiliary data)
        .synthesizeWith[(Vector[u.TermSymbol], Map[u.TermSymbol, Set[u.TermSymbol]])] {
          case Attr.none(api.ValRef(sym)) if funs contains sym =>
            (Vector(sym), Map())
          case Attr(api.ValDef(sym, rhs, _), _, _, syn) if isFun(sym) =>
            (Vector(), Map(sym -> syn(rhs).head._1.toSet))
        }
        // Am I inside a lambda?
        .inherit {
          case api.Lambda(_,_,_) => true
        }(Monoids.disj)
        // Am I inside a combinator call?
        .inherit {
          case api.DefCall(_, method, _, _) if combinators contains method => true
        }(Monoids.disj)
        // Funs given as arguments to combinators (combRefs)
        .accumulateWith[Vector[u.TermSymbol]] {
          case Attr.inh(api.ValRef(sym), insideCombinator :: _)
            if insideCombinator && (funs contains sym) => Vector(sym)
        }
        // Funs referenced from top-level (topLevRefs)
        .accumulateWith[Vector[u.TermSymbol]] {
          case Attr.inh(api.ValRef(sym), insideCombinator :: insideLambda :: _)
            if !insideLambda && !insideCombinator && (funs contains sym) =>
            Vector(sym)
        }
          .traverse { PartialFunction.empty }(tree)

      def funReachability(start: Vector[u.TermSymbol]): Set[u.TermSymbol] = {
        var reached = start.toSet
        var frontier = start
        while (frontier.nonEmpty) {
          frontier = for {
            u <- frontier
            v <- funGraph(u)
            if !reached(v)
          } yield v
          reached ++= frontier
        }
        reached
      }

      // The Set of symbols of ValDefs of functions
      val lambdas = tree.collect {
        case api.ValDef(sym, api.Lambda(_,_,_), _) => sym
      }.toSet

      val driverFuns = funReachability(topLevRefs)
      // highFuns0 will also contain the ambiguous ones, which we will soon eliminate
      val highFuns0 = funReachability(combRefs)
      val ambiguousFuns = driverFuns intersect highFuns0
      // Create a map from the ambiguous lambdas to the newly created $high versions
      val ambiguousFunMap = Map((ambiguousFuns map {
        sym => sym -> api.TermSym.free(api.TermName.fresh(sym.name.toString ++ "$high"), sym.typeSignature)
      }).toSeq: _*)
      val newFuns = ambiguousFunMap.values
      val highFuns = highFuns0 -- ambiguousFuns ++ newFuns

      // Note that if a fun is defined inside a high lambda, then it won't be in highFuns,
      // because of the refresh. But this is not a problem, since isHighContext will be inherited to it anyway.
      val isHighContext: u.Tree =?> Boolean = {
        case api.ValDef(sym, _, _) if highFuns contains sym => true
        case api.DefCall(_, method, _, _) if combinators contains method => true
      }

      def refreshAllValsAndLambdas(tree: u.Tree): u.Tree = {
        api.Tree.refresh(tree.collect{
          case api.ValDef(sym, _, _) => Seq(sym)
          case api.Lambda(sym, params, _) =>
            Seq(sym) ++ (params map { case core.ParDef(sym2, _, _) => sym2 })
        }.flatten: _*)(tree)
      }

      val disambiguatedTree = api.TopDown
        .inherit(isHighContext)(Monoids.disj)
        .transformWith {

          case Attr.none(core.Let(vals, defs, expr)) =>
            val newVals = vals flatMap { case v@api.ValDef(lhs, rhs, flags) =>
              if (ambiguousFuns contains lhs) {
                Seq(v, api.ValDef(ambiguousFunMap(lhs), refreshAllValsAndLambdas(rhs), flags))
              } else {
                Seq(v)
              }
            }
            core.Let(newVals: _*)(defs: _*)(expr)

          case Attr.inh(api.ValRef(sym), true :: _) if ambiguousFuns(sym) => api.ValRef(ambiguousFunMap(sym))

        }(tree).tree

      (disambiguatedTree, isHighContext)
    }

  }
}
