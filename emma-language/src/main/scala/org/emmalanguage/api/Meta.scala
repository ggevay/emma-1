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
package api

import scala.reflect.ClassTag

import scala.language.implicitConversions

// -----------------------------------------------------
// types supported by Emma
// -----------------------------------------------------

object Meta {

  object Projections {
    implicit def ctagFor[T](implicit meta: Meta[T]): ClassTag[T] =
      ClassTag[T](meta.mirror.runtimeClass(meta.tpe))



//    implicit def aaa[T](t: (scala.reflect.api.JavaUniverse)#TypeTag[T]): reflect.runtime.universe.TypeTag[T] = {
//      t.asInstanceOf[reflect.runtime.universe.TypeTag[T]]
//    }


//    implicit def aaa[T](t: (scala.reflect.api.JavaUniverse)#TypeTag[T]): Meta[T] = {
//      t.asInstanceOf[reflect.runtime.universe.TypeTag[T]]
//    }

    def bb() = {}

    // https://github.com/epfldata/squid/issues/57#issuecomment-396255998
    // But it's not working: It's not finding this implicit conversion, and I have zero idea why.
    //  (I even tried adding import Meta.Projections._ inside the code of the first test in BaseCodegenIntegrationSpec)
    implicit def aaa(t: (scala.reflect.api.JavaUniverse)#TypeTag[String]): org.emmalanguage.api.Meta[String] = {
      t.asInstanceOf[reflect.runtime.universe.TypeTag[String]]
    }
  }

}
