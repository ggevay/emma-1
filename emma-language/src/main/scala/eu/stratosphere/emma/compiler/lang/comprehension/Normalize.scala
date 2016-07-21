package eu.stratosphere.emma.compiler.lang.comprehension

import eu.stratosphere.emma.compiler.lang.core.Core
import eu.stratosphere.emma.compiler.{Common, Rewrite}

private[comprehension] trait Normalize extends Common
  with Rewrite {
  self: Core with Comprehension =>

  import UniverseImplicits._
  import Comprehension.{Syntax, asLet}
  import Core.{Lang => core}

  private[comprehension] object Normalize {

    /**
     * Normalizes nested mock-comprehension syntax.
     *
     * @param monad The [[Symbol]] of the monad syntax to be normalized.
     * @param tree  The [[Tree]] to be resugared.
     * @return The input [[Tree]] with resugared comprehensions.
     */
    def normalize(monad: u.Symbol)(tree: u.Tree): u.Tree = {
      // construct comprehension syntax helper for the given monad
      val cs = new Syntax(monad: u.Symbol) with NormalizationRules

      ({
        // apply UnnestHead and UnnestGenerator rules exhaustively
        Engine.bottomUp(List(cs.UnnestHead, cs.UnnestGenerator))
      } andThen {
        // elminiate dead code produced by normalization
        Core.dce
      } andThen {
        // elminiate trivial guards produced by normalization
        tree => api.BottomUp.transform {
          case cs.comprehension(qs, hd) =>
            cs.comprehension(
              qs filterNot {
                case t@cs.guard(core.Let(_, _, Nil, core.Lit(true))) => true
                case t => false
              }, hd)
        }(tree).tree
      }) (tree)
    }
  }

  protected trait NormalizationRules {
    self: Syntax =>

    // -------------------------------------------------------------------------
    // Comprehension normalization rules
    // -------------------------------------------------------------------------

    /**
     * Unnests a comprehended head in its parent.
     *
     * ==Matching Pattern==
     * {{{
     * {
     *   $vals1
     *   flatten {
     *     $vals2
     *     comprehension {
     *       $qs1
     *       head {
     *         $vals3
     *         comprehension {
     *           $qs2
     *           head $hd2
     *         } // child comprehension
     *       }
     *     } // outer comprehension
     *   } // parent expression
     * } // enclosing block
     * }}}
     *
     * ==Guard==
     * None.
     *
     * ==Rewrite==
     *
     * Let $vals3 decompose into the following two subsets:
     * - $vals3i (transitively) depends on symbols defined in $qs1, and
     * - $vals3o is the independent complement $vals3 \ $vals3o.
     *
     * {{{
     * {
     *   $vals1
     *   $vals2
     *   $vals3o
     *   comprehension {
     *     $qs1
     *     $qs2' // where let blocks are prefixed with $vals3i
     *     head $hd' // where let blocks are prefixed with $vals3i
     *   } // flattened result comprehension
     * } // enclosing block
     * }}}
     */
    object UnnestHead extends Rule {

      override def apply(root: u.Tree): Option[u.Tree] = root match {
        //@formatter:off
        case core.Let(
          vals1,
          defs1,
          effs1,
          expr1@flatten(
            core.Let(
              vals2,
              Nil,
              Nil,
              expr2@comprehension(
                qs1,
                head(
                  core.Let(
                    vals3,
                    Nil,
                    Nil,
                    expr3@comprehension(
                      qs2,
                      head(hd2)
                    ))))))) =>
          //@formatter:on

          val (vals3i, vals3o) = split(vals3, qs1)

          val qs2p = qs2 map {
            case generator(sym, rhs) =>
              generator(sym, prepend(vals3i, rhs))
            case guard(expr) =>
              guard(prepend(vals3i, expr))
          }

          val hd2p = prepend(vals3i, hd2)

          // API: cumbersome syntax of Let.apply
          Some(core.Let(
            vals1 ++ vals2 ++ vals3o: _*
          )(
            defs1: _*
          )(
            effs1: _*
          )(
            comprehension(qs1 ++ qs2p, head(hd2p))
          ))

        case _ =>
          // subtree's root does not match, don't modify the subtree
          None
      }

      /** Splits `vals` in two subsequences: vals dependent on generators bound in `qs`, and complement. */
      private def split(vals: Seq[u.ValDef], qs: Seq[u.Tree]): (Seq[u.ValDef], Seq[u.ValDef]) = {
        // symbols referenced in vals
        val vals3refs = (for {
          core.ValDef(sym, rhs, _) <- vals
        } yield sym -> api.Tree.refs(rhs)).toMap

        // symbols defined in qs
        val qs1Syms = (for {
          generator(sym, _) <- qs
        } yield sym).toSet

        // symbols defined in vals3 which directly depend on symbols from qs1
        var vasDepSyms = (for {
          (sym, refs) <- vals3refs
          if (refs intersect qs1Syms).nonEmpty
        } yield sym).toSet

        // compute the transitive closure of vals3iSyms, i.e. extend with indirect dependencies
        var delta = Set.empty[u.TermSymbol]
        do {
          vasDepSyms = vasDepSyms union delta
          delta = (for {
            (sym, refs) <- vals3refs
            if (refs intersect vasDepSyms).nonEmpty
          } yield sym).toSet diff vasDepSyms
        } while (delta.nonEmpty)

        // symbols defined in vals3 which do not depend on symbols from qs1
        val valsIndSyms = vals3refs.keySet diff vasDepSyms

        val vasDep = for {
          vd@core.ValDef(sym, rhs, _) <- vals
          if vasDepSyms contains sym
        } yield vd

        val valsInd = for {
          vd@core.ValDef(sym, rhs, _) <- vals
          if valsIndSyms contains sym
        } yield vd

        (vasDep, valsInd)
      }

      private def prepend(prefix: Seq[u.ValDef], blck: u.Block): u.Block = blck match {
        case core.Let(vals, defs, effs, expr) =>
          val prefixSyms = prefix collect {
            case core.ValDef(sym, _, _) => sym
          }

          api.Tree.refresh(
            prefixSyms: _*
          )(
            core.Let(prefix ++ vals: _*)(defs: _*)(effs: _*)(expr)
          ).asInstanceOf[u.Block]
      }
    }

    /**
     * Un-nests a comprehended generator in its parent.
     *
     * ==Matching Pattern==
     * {{{
     * {
     *   $vals1a
     *   val y = comprehension {
     *     $qs2
     *     head $hd2
     *   } // child comprehension
     *   $vals1b
     *   comprehension {
     *     $qs1a
     *     val x = generator(y) // gen
     *     $qs1b
     *     head $hd1
     *   } // parent comprehension
     * } // enclosing block
     * }}}
     *
     * ==Guard==
     * None.
     *
     * ==Rewrite==
     * {{{
     * {
     *   $vals1a
     *   $vals1b
     *   comprehension {
     *     $qs1a
     *     $qs2
     *     $qs1b [ $hd2 \ x ]
     *     head $hd1 [ $hd2 \ x ]
     *   } // unnested result comprehension
     * } // enclosing block
     * }}}
     */
    object UnnestGenerator extends Rule {

      override def apply(root: u.Tree): Option[u.Tree] = root match {
        //@formatter:off
        case core.Let(
          vals1,
          defs1,
          effs1,
          comprehension(
            qs1,
            head(hd1))) =>
          //@formatter:on

          val compMap = (for {
            vd@core.ValDef(y, comprehension(_, head(_)), _) <- vals1
          } yield y -> vd).toMap

          qs1 collectFirst {
            case gen@generator(x, core.Let(Nil, Nil, Nil, core.ValRef(y))) if compMap.isDefinedAt(y) =>

              // destruct the nested comprehension ValDef
              val vd@core.ValDef(_, comprehension(qs2, head(hd2)), _) = compMap(y)

              // define a substitution function `· [ $hd2 \ x ]`
              val subst = hd2 match {
                case core.Let(Nil, Nil, Nil, expr) => api.Tree.subst(x -> expr)
                case _ => api.Tree.subst(x -> hd2)
              }

              // compute prefix and suffix for qs1 and vals1
              val (qs1a, qs1b) = splitAt[u.Tree](gen)(qs1)
              val (vals1a, vals2b) = splitAt(vd)(vals1)

              // API: cumbersome syntax of Let.apply
              core.Let(
                vals1a ++ vals2b: _*
              )(
                defs1: _*
              )(
                effs1: _*
              )(
                comprehension(
                  qs1a ++ qs2 ++ (qs1b map subst),
                  head(asLet(subst(hd1)))))
          }

        case _ =>
          None
      }

      /* Splits a `Seq[A]` into a prefix and suffix. */
      private def splitAt[A](e: A): Seq[A] => (Seq[A], Seq[A]) = {
        (_: Seq[A]).span(_ != e)
      } andThen {
        case (pre, _ :: suf) => (pre, suf)
      }
    }

  }

}
