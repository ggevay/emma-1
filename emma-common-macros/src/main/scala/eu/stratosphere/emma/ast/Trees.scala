package eu.stratosphere.emma
package ast

import cats.std.all._
import shapeless._

import scala.annotation.tailrec

trait Trees { this: AST =>

  trait TreeAPI { this: API =>

    import universe._
    import u.internal.substituteSymbols

    object Tree extends Node {

      // Predefined trees
      lazy val Root = Id(u.rootMirror.RootPackage)
      lazy val Java = Sel(Root, u.definitions.JavaLangPackage)
      lazy val Scala = Sel(Root, u.definitions.ScalaPackage)

      /** Creates a copy of `tree` while preserving its node type and setting new attributes. */
      def copy[T <: u.Tree](tree: T)(
        pos: u.Position = tree.pos,
        sym: u.Symbol = tree.symbol,
        tpe: u.Type = tree.tpe): T = {

        val copy = tree.duplicate.asInstanceOf[T]
        if (is.defined(sym)) set.sym(copy, sym)
        if (is.defined(tpe)) set.tpe(copy, Type.fix(tpe))
        if (is.defined(pos)) set.pos(copy, pos)
        copy
      }

      /** Prints `tree` for debugging (most details). */
      def debug(tree: u.Tree): String =
        u.showCode(tree,
          printIds = true,
          printOwners = true,
          printTypes = true)

      /** Prints `tree` in parseable form. */
      def show(tree: u.Tree): String =
        u.showCode(tree, printRootPkg = true)

      /** Prints `tree` including owners as comments. */
      def showOwners(tree: u.Tree): String =
        u.showCode(tree, printOwners = true)

      /** Prints `tree` including symbols as comments. */
      def showSymbols(tree: u.Tree): String =
        u.showCode(tree, printIds = true)

      /** Prints `tree` including types as comments. */
      def showTypes(tree: u.Tree): String =
        u.showCode(tree, printTypes = true)

      /** Parses a snippet of source `code` into a type-checked AST. */
      def parse(code: String): u.Tree =
        Type.check(Trees.this.parse(code))

      /** Returns a set of all term definitions in a `tree`. */
      def defs(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case TermDef(lhs) => lhs
      }.toSet

      /** Returns a set of all term references in a `tree`. */
      def refs(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case TermRef(target) => target
      }.toSet

      /** Returns the closure of `tree` as a set. */
      def closure(tree: u.Tree): Set[u.TermSymbol] =
        refs(tree) diff defs(tree)

      /** Returns a set of all binding definitions in `tree`. */
      def bindings(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case BindingDef(lhs, _, _) => lhs
      }.toSet

      /** Returns a set of all lambdas in `tree`. */
      def lambdas(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case Lambda(fun, _, _) => fun
      }.toSet

      /** Returns a set of all method (`def`) definitions in `tree`. */
      def methods(tree: u.Tree): Set[u.MethodSymbol] = tree.collect {
        case (_: u.DefDef) withSym (method: u.MethodSymbol) => method
      }.toSet

      /** Returns a set of all variable (`var`) mutations in `tree`. */
      def mutations(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case VarMut(lhs, _) => lhs
      }.toSet

      /** Returns the subset of `closure(tree)` that is modified within `tree`. */
      def closureMod(tree: u.Tree): Set[u.TermSymbol] =
        closure(tree) & mutations(tree)

      /** Returns a set of all parameter definitions in `tree`. */
      def parameters(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case ParDef(lhs, _, _) => lhs
      }.toSet

      /** Returns a set of all value (`val`) definitions in `tree`. */
      def values(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case ValDef(lhs, _, _) => lhs
      }.toSet

      /** Returns a set of all variable (`var`) definitions in `tree`. */
      def variables(tree: u.Tree): Set[u.TermSymbol] = tree.collect {
        case VarDef(lhs, _, _) => lhs
      }.toSet

      /** Returns a fully-qualified reference to `target` (must be static). */
      def resolveStatic(target: u.Symbol): u.Tree = {
        assert(is.defined(target), s"Cannot resolve undefined target `$target`")
        assert(target.isStatic, s"Cannot resolve dynamic target `$target`")
        Owner.chain(target).init.foldRight[u.Tree](Root) {
          (member, owner) => Sel(owner, member)
        }
      }

      /** Inlines a sequence of binding definitions in a tree by replacing LHS with RHS. */
      def inline(bindings: u.ValDef*): u.Tree => u.Tree =
        if (bindings.isEmpty) identity else {
          val dict = bindings.map(bind => bind.symbol -> bind.rhs).toMap
          TopDown.break.transform {
            case BindingDef(lhs, _, _) if dict contains lhs => Term.unit
            case TermRef(target) if dict contains target => dict(target)
          }.andThen(_.tree)
        }

      /** Replaces a sequence of `symbols` in a tree with freshly named ones. */
      def refresh(symbols: u.Symbol*): u.Tree => u.Tree =
        rename(symbols.map(sym => sym -> {
          if (is.term(sym)) TermSym.fresh(sym.asTerm)
          else TypeSym.fresh(sym.asType)
        }): _*)

      /** Replaces a sequence of term symbols with references to their `aliases`. */
      def rename(aliases: (u.Symbol, u.Symbol)*): u.Tree => u.Tree =
        if (aliases.isEmpty) identity else {
//          val (from, to) = aliases.toList.unzip
//          tree => substituteSymbols(Tree.copy(tree)(), from, to)
          val m = Map(aliases: _*)
          TopDown.transform {
            case api.Id(sym) if m contains sym => api.Id(m(sym))
            // TODO: We don't need to handle Ref, ValRef, and VarRef, because we are handling Id, right?
//            case api.ValRef(sym) if m contains sym => api.ValRef(m(sym).asInstanceOf[TermSymbol])
//            case api.VarRef(sym) if m contains sym => api.VarRef(m(sym).asInstanceOf[TermSymbol])
            // TODO: Do we want to handle api.Lambda? (It's parameters will be handled anyway, it's just its sym)
//            case api.Lambda(sym, params, body) => {
//              assert(!(m contains sym), s"Tried to substitute symbol of Lambda")
//              api.Lambda((params map {case api.ValDef(sym,)}): _*)(body)
//            }
            // TODO: nothing to do with case api.Def, right?
            case api.Sel(target, member) if m contains member => api.Sel(target, m(member))
            // TODO: We don't need to handle api.Acc, because we are handling api.Sel
            //case api.Acc(target, member) if m contains member => api.Acc(target, m(member))
            case api.BindingDef(lhs, rhs, flags) if m contains lhs =>
              api.BindingDef(m(lhs).asInstanceOf[u.TermSymbol], rhs, flags)
            case api.BindingRef(sym) if m contains sym => api.BindingRef(m(sym).asInstanceOf[u.TermSymbol])
            // We are covering BindingSym by handling BindingDef and BindingRef
            // ValSym and VarSym are handled by BindingRef and BindingDef
            case api.PatAt(lhs, rhs) if m contains lhs => api.PatAt(m(lhs).asInstanceOf[u.TermSymbol], rhs)
            // PatConst, PatQual, PatVar are handled already
            case api.TermAcc(target, member) if m contains member =>
              api.TermAcc(target, m(member).asInstanceOf[u.TermSymbol])
            case api.DefCall(target, method, targs, argss@_*) if m contains method =>
              api.DefCall(target)(m(method).asInstanceOf[u.TermSymbol], targs: _*)(argss: _*)
            case api.DefDef(sym, flags, tparams, paramss@_*, body@_*) if m contains sym =>
              api.DefDef(m(sym).asInstanceOf[u.MethodSymbol], flags)(tparams: _*)(
                paramss.map{_.map{case api.ValDef(s, _, _) => s}}: _*)(body)
            // DefSym is covered by DefCall and DefDef
            //TODO: how to handle LabelSym?
            case api.ModuleAcc(target, member) if m contains member =>
              api.ModuleAcc(target, m(member).asInstanceOf[u.ModuleSymbol])
            case api.ModuleRef(sym) if m contains sym => api.ModuleRef(m(sym).asInstanceOf[u.ModuleSymbol])
            // ParRef and ParDef are handled by BindingRef and BindingDef
            //case api.TypeSym

            //TODO: just handle BindingSym, DefSym, ModuleSym, TypeSym   (apply using m(sym).name, .tpe, .flags, etc.)
            //TODO: handle .name

          }.andThen(_.tree)
        }

      /** Replaces occurrences of `find` with `repl` in a tree. */
      def replace(find: u.Tree, repl: u.Tree): u.Tree => u.Tree =
        TopDown.break.transform {
          case tree if tree equalsStructure find => repl
        }.andThen(_.tree)

      /** Substitutes a sequence of symbol-value pairs in a tree. */
      def subst(kvs: (u.Symbol, u.Tree)*): u.Tree => u.Tree =
        subst(kvs.toMap)

      /** Substitutes a dictionary of symbol-value pairs in a tree. */
      def subst(dict: Map[u.Symbol, u.Tree]): u.Tree => u.Tree =
        if (dict.isEmpty) identity else {
          val closure = dict.values
            .flatMap(this.closure)
            .filterNot(dict.keySet)
            .map(_.name).toSet

          TopDown.break.accumulate(Attr.collect[Set, u.TermSymbol] {
            case TermDef(lhs) => lhs
          }).transform { case TermRef(target) if dict contains target =>
            dict(target)
          }.andThen { case Attr.acc(tree, defs :: _) =>
            val capture = for (d <- defs if closure(d.name)) yield d
            refresh(capture.toSeq: _*)(tree)
          }
        }

      /** Reverses eta-expansions in `tree`. */
      def etaCompact(tree: u.Tree): u.Tree = Tree.inline(tree.collect {
        case eta @ ParDef(_ withName TermName.Eta(_), _, _) => eta
      }: _*)(tree)

      /** Creates a new lambda from a `method` reference with an optional `target`. */
      def etaExpand(target: Option[u.Tree] = None)(method: u.MethodSymbol): u.Function = {
        assert(is.defined(method), s"Cannot eta-expand undefined method `$method`")
        assert(is.overloaded(method), s"Cannot eta-expand overloaded method `$method`")
        assert(has.tpe(method), s"Method `$method` has no type")

        lazy val tpe = target match {
          case Some(_ withType t) => Type.of(method, in = t)
          case _ => Type.of(method)
        }

        assert(!is.poly(tpe), s"Cannot eta-expand polymorphic method `$method` of type `$tpe`")

        val paramss = for (params <- method.paramLists) yield
          for (p <- params) yield TermSym.free(TermName.Eta(), p.info, get.flags(p))

        val args = for (params <- paramss) yield
          for (p <- params) yield TermRef(p)

        Lambda(paramss.flatten: _*) {
          DefCall(target)(method)(args: _*)
        }
      }

      /** Creates a curried version of the supplied `lambda`. */
      def curry(lambda: u.Function): u.Function = lambda match {
        case Lambda(sym, params, body) => params.foldRight(body) {
          case (ParDef(lhs, _, _), rhs) => Lambda(lhs)(rhs)
        }.asInstanceOf[u.Function]
      }

      /** Removes all (possibly nested) type ascriptions from `tree`. */
      @tailrec
      def unAscribe(tree: u.Tree): u.Tree = tree match {
        case TypeAscr(expr, _) => unAscribe(expr)
        case _ => tree
      }
    }

    /** The empty tree (instance independent). */
    object Empty extends Node {

      def apply(): u.Tree =
        u.EmptyTree

      def unapply(tree: u.Tree): Option[u.Tree] =
        Option(tree).filter(_.isEmpty)
    }

    /** Identifiers (for internal use). */
    private[ast] object Id extends Node {

      def apply(target: u.Symbol): u.Ident = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(has.name(target), s"$this target `$target` has no name")
        assert(has.tpe(target), s"$this target `$target` has no type")
        assert(is.encoded(target), s"$this target `$target` is not encoded")

        val tpe = Type.of(target) match {
          case u.NullaryMethodType(result) => result
          case other => other
        }

        val id = u.Ident(target.name)
        set(id, sym = target, tpe = tpe)
        id
      }

      def unapply(id: u.Ident): Option[u.Symbol] = id match {
        case _ withSym target => Some(target)
        case _ => None
      }
    }

    /** Selections (for internal use). */
    private[ast] object Sel extends Node {

      def apply(target: u.Tree, member: u.Symbol): u.Select = {
        assert(is.defined(target), s"$this target is not defined: $target")
        assert(has.tpe(target), s"$this target has no type:\n${Tree.showTypes(target)}")
        assert(is.defined(member), s"$this member `$member` is not defined")
        assert(has.tpe(member), s"$this member `$member` has no type")

        val mod = member.isPackageClass || member.isModuleClass
        val sym = if (mod) member.asClass.module else member
        val tpe = Type.of(sym, in = target.tpe) match {
          case u.NullaryMethodType(result) => result
          case other => other
        }

        val sel = u.Select(target, sym.name)
        set(sel, sym = sym, tpe = tpe)
        sel
      }

      def unapply(sel: u.Select): Option[(u.Tree, u.Symbol)] = sel match {
        case u.Select(target, _) withSym member => Some(target, member)
        case _ => None
      }
    }

    /** References. */
    object Ref extends Node {

      /**
       * Creates a type-checked reference.
       * @param target Cannot be a method or package.
       * @return `target`.
       */
      def apply(target: u.Symbol): u.Ident = {
        assert(is.defined(target), s"$this target `$target` is not defined")
        assert(!is.method(target), s"$this target `$target` cannot be a method")
        assert(!is.pkg(target), s"$this target `$target` cannot be a package")
        Id(target)
      }

      def unapply(ref: u.Ident): Option[u.Symbol] = ref match {
        case Id(target) if !is.method(target) && !is.pkg(target) => Some(target)
        case _ => None
      }
    }

    /** Member accesses. */
    object Acc extends Node {

      /**
       * Creates a type-checked member access.
       * @param target Must be a term.
       * @param member Must be a dynamic symbol.
       * @return `target.member`.
       */
      def apply(target: u.Tree, member: u.Symbol): u.Select = {
        assert(is.defined(target), s"$this target is not defined: $target")
        assert(is.term(target), s"$this target is not a term:\n${Tree.show(target)}")
        assert(is.defined(member), s"$this member `$member` is not defined")
        assert(!member.isStatic, s"$this member `$member` cannot be static")
        assert(!is.method(member), s"$this member `$member` cannot be a method")
        Sel(target, member)
      }

      def unapply(acc: u.Select): Option[(u.Tree, u.Symbol)] = acc match {
        case Sel(Term(target), member) if !member.isStatic && !is.method(member) =>
          Some(target, member)
        case _ => None
      }
    }

    /** Definitions. */
    object Def extends Node {
      def unapply(tree: u.Tree): Option[u.Symbol] = for {
        tree <- Option(tree)
        if tree.isDef && has.sym(tree)
      } yield tree.symbol
    }
  }
}
