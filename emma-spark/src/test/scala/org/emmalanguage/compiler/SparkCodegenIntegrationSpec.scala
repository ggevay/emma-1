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

import api._
import api.spark._

import org.apache.spark.rdd.RDD

class SparkCodegenIntegrationSpec extends BaseCodegenIntegrationSpec
  with SparkCompilerAware
  with SparkAware {

  import compiler._

  def withBackendContext[T](f: Env => T): T =
    withDefaultSparkSession(f)

  // --------------------------------------------------------------------------
  // Distributed collection conversion
  // --------------------------------------------------------------------------

  "Convert from/to a Spark RDD" in withBackendContext(implicit env => {
    verify(u.reify {
      val xs = DataBag(1 to 1000).withFilter(_ > 800)
      val ys = xs.as[RDD].filter(_ < 200)
      val zs = DataBag.from(ys)
      zs.size
    })
  })

  "aaaaa" in {

    println(Source.valid(u.reify {
      //      val a = 5

      //      val x = {
      //        var y = 5
      //        while(y>1){
      //          y-=1
      //        }
      //      }
      //      x

//      {
//        val x = 7
//        x
//      }

      println(5)


      //8


    }.tree))
  }



  "bbbb" in {
    val tree = u.reify {
      val x = {
        var y = 5
        while(y>0){
          y-=1
        }
        y
      }
      x
    }

    println(actPipeline(tree))
    println(u.showCode(actPipeline(tree)))
    //println(Tree.debug(actPipeline(tree)))

  }


  "ccc" in {

    println({
      var y$p$r2 = 5;
      def while$r2(): Unit ={
        if ({
          val anf$r3 = y$p$r2.>(0);
          anf$r3
        })
        {
          {
            val anf$r2 = y$p$r2.-(1);
            y$p$r2 = anf$r2;
            ()
          };
          while$r2()
        }
        else
          ()
      };
      y$p$r2
    })

  }



  "dddd" in withBackendContext(implicit env => {
    verify(u.reify {

      val x = {
        var y = 5
        while(y>0){
          y-=1
        }
        y
      }
      x

    })
  })


}
