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




//  {
//    val fun$r179 = ((env$r87: _root_.org.apache.spark.sql.SparkSession) => {
//      implicit val e$r87 = env$r87;
//      val anf$r708 = _root_.scala.Predef.intWrapper(1);
//      val anf$r709 = anf$r708.to(100);
//      val anf$r710 = _root_.org.emmalanguage.api.SparkDataset.apply[_root_.scala.Int](anf$r709);
//      val f$r127 = ((_$r1: _root_.scala.Int) => {
//        1
//      });
//      val as = anf$r710.map[_root_.scala.Int](f$r127);
//      val anf$r712 = _root_.scala.Predef.intWrapper(101);
//      val anf$r713 = anf$r712.to(200);
//      val anf$r714 = _root_.org.emmalanguage.api.SparkDataset.apply[_root_.scala.Int](anf$r713);
//      val anf$r715 = _root_.scala.Predef.intWrapper(2);
//      val anf$r716 = anf$r715.to(4);
//      val anf$r717 = _root_.org.emmalanguage.api.SparkDataset.apply[_root_.scala.Int](anf$r716);
//      val crossed$r1 = _root_.org.emmalanguage.api.spark.SparkNtv.cross[_root_.scala.Int, _root_.scala.Int](anf$r714, anf$r717);
//      val f$r132 = ((xy$r2: _root_.org.emmalanguage.api.spark.SparkExp.Root) => {
//        val g = _root_.org.emmalanguage.api.spark.SparkExp.proj(xy$r2, "_2");
//        g
//      });
//      val bs = _root_.org.emmalanguage.api.spark.SparkNtv.project[_root_.scala.Tuple2[_root_.scala.Int, _root_.scala.Int], _root_.scala.Int](f$r132)(crossed$r1);
//      val anf$r719 = _root_.scala.Predef.intWrapper(201);
//      val anf$r720 = anf$r719.to(300);
//      val anf$r721 = _root_.org.emmalanguage.api.SparkDataset.apply[_root_.scala.Int](anf$r720);
//      val p$r29 = ((_ : _root_.scala.Int) => {
//        false
//      });
//      val filtered$r22 = anf$r721.withFilter(p$r29);
//      val f$r133 = ((_ : _root_.org.emmalanguage.api.spark.SparkExp.Root) => {
//        _
//      });
//      val anf$r722 = _root_.org.emmalanguage.api.spark.SparkNtv.project[_root_.scala.Int, _root_.scala.Int](f$r133)(filtered$r22);
//      val f$r130 = ((_$r2: _root_.scala.Int) => {
//        5
//      });
//      val cs = anf$r722.map[_root_.scala.Int](f$r130);
//      val anf$r724 = _root_.scala.Predef.intWrapper(301);
//      val anf$r725 = anf$r724.to(400);
//      val anf$r726 = _root_.org.emmalanguage.api.SparkDataset.apply[_root_.scala.Int](anf$r725);
//      val p$r30 = ((x$60: _root_.scala.Int) => {
//        true
//      });
//      val filtered$r23 = anf$r726.withFilter(p$r30);
//      val f$r134 = ((x$60: _root_.org.emmalanguage.api.spark.SparkExp.Root) => {
//        x$60
//      });
//      val ds = _root_.org.emmalanguage.api.spark.SparkNtv.project[_root_.scala.Int, _root_.scala.Int](f$r134)(filtered$r23);
//      val anf$r728 = as.union(bs);
//      val anf$r729 = anf$r728.union(cs);
//      val anf$r730 = anf$r729.union(ds);
//      anf$r730
//    });
//    fun$r179
//  }




}
