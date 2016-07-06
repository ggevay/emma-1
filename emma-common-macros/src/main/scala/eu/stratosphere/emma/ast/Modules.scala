package eu.stratosphere
package emma.ast

/** Modules (`object`s). */
trait Modules { this: AST =>

  /** Modules (`object`s). */
  trait ModuleAPI { this: API =>

    import universe._
    import u.Flag._
    import u.internal.newModuleAndClassSymbol
    import u.internal.singleType

    /** Module (`object`) symbols. */
    object ModuleSym extends Node {

      /**
       * Creates a new module symbol.
       * @param owner The enclosing named entity where this module is defined.
       * @param name The name of this module (will be encoded).
       * @param flags Any additional modifiers (cannot be mutable or parameter).
       * @param pos The (optional) source code position where this module is defined.
       * @return A new module symbol.
       */
      def apply(owner: u.Symbol, name: u.TermName,
        flags: u.FlagSet = u.NoFlags,
        pos: u.Position = u.NoPosition): u.ModuleSymbol = {

        assert(is.defined(name), s"$this name `$name` is not defined")
        assert(are.not(MUTABLE)(flags), s"$this `$name` cannot be mutable")
        assert(are.not(PARAM)(flags), s"$this `$name` cannot be a parameter")

        val module = newModuleAndClassSymbol(owner, TermName(name), pos, flags)._1
        set.tpe(module, singleType(u.NoPrefix, module))
        module
      }

      def unapply(sym: u.ModuleSymbol): Option[u.ModuleSymbol] =
        Option(sym)

      /** Extractor for dynamic module symbols (local and class members). */
      object Dynamic {
        def unapply(sym: u.ModuleSymbol): Option[u.ModuleSymbol] =
          Option(sym).filterNot(_.isStatic)
      }

      /** Extractor for static module symbols (object and package object members). */
      object Static {
        def unapply(sym: u.ModuleSymbol): Option[u.ModuleSymbol] =
          Option(sym).filter(_.isStatic)
      }
    }

    /** Module (`object`) references. */
    object ModuleRef extends Node {

      /**
       * Creates a type-checked module reference.
       * @param target Must be a module symbol.
       * @return `target`.
       */
      def apply(target: u.ModuleSymbol): u.Ident =
        TermRef(target)

      def unapply(ref: u.Ident): Option[u.ModuleSymbol] = ref match {
        case TermRef(ModuleSym(target)) => Some(target)
        case _ => None
      }

      /** Dynamic module references (local and class members). */
      object Dynamic extends Node {

        /**
         * Creates a type-checked dynamic module reference.
         * @param target Must be a dynamic module symbol.
         * @return `target`.
         */
        def apply(target: u.ModuleSymbol): u.Ident = {
          assert(is.defined(target), s"$this target `$target` is not defined")
          assert(!target.isStatic, s"$this target `$target` cannot be static")
          ModuleRef(target)
        }

        def unapply(ref: u.Ident): Option[u.ModuleSymbol] = ref match {
          case ModuleRef(ModuleSym.Dynamic(target)) => Some(target)
          case _ => None
        }
      }

      /** Static module references (top-level / object or package object members). */
      object Static extends Node {

        /**
         * Creates a type-checked static module reference.
         * @param target Must be a static module symbol.
         * @return `target`.
         */
        def apply(target: u.ModuleSymbol): u.Ident = {
          assert(is.defined(target), s"$this target `$target` is not defined")
          assert(target.isStatic, s"$this target `$target` is not static")
          ModuleRef(target)
        }

        def unapply(ref: u.Ident): Option[u.ModuleSymbol] = ref match {
          case ModuleRef(ModuleSym.Static(target)) => Some(target)
          case _ => None
        }
      }
    }

    /** Module (`object`) accesses. */
    object ModuleAcc extends Node {

      /**
       * Creates a type-checked module access.
       * @param target Must be a term.
       * @param member Must be a dynamic module symbol.
       * @return `target.member`.
       */
      def apply(target: u.Tree, member: u.ModuleSymbol): u.Select =
        TermAcc(target, member)

      def unapply(acc: u.Select): Option[(u.Tree, u.ModuleSymbol)] = acc match {
        case TermAcc(target, ModuleSym(member)) => Some(target, member)
        case _ => None
      }
    }
  }
}
