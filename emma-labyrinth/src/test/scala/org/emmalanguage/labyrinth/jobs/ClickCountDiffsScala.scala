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
package labyrinth.jobs

import labyrinth.CFLConfig
import labyrinth.ElementOrEvent
import labyrinth.KickoffSource
import labyrinth.LabyNode
import labyrinth.LabySource
import labyrinth.operators.CFAwareFileSink
import labyrinth.operators.ClickLogReader
import labyrinth.operators.ConditionNode
import labyrinth.partitioners.Always0
import labyrinth.partitioners.Forward
import labyrinth.partitioners.RoundRobin
import labyrinth.util.TupleIntInt
import labyrinth.operators._
import org.emmalanguage.labyrinth.partitioners.LongBy0
import org.emmalanguage.labyrinth.partitioners.TupleLongLongBy0
import org.emmalanguage.labyrinth.util.TupleLongLong

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TupleCsvInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ClickCountDiffsScala {
  private val typeInfoTupleLongLong =
		new TupleTypeInfo[Tuple2[Long, Long]](
			TypeInformation.of(classOf[Long]),
			TypeInformation.of(classOf[Long]))

  // TODO don't use classOF
  private val longSer = TypeInformation.of(classOf[java.lang.Long]).createSerializer(new ExecutionConfig)
  private val booleanSer = TypeInformation.of(classOf[java.lang.Boolean]).createSerializer(new ExecutionConfig)
  // TODO use TypeHint instead
  private val tupleLongLongSer = new TupleLongLong.TupleLongLongSerializer
  private val tuple2LongLongSerializer = TypeInformation.of(new TypeHint[Tuple2[TupleLongLong, TupleLongLong]]() {})
		.createSerializer(new ExecutionConfig)

  @throws[Exception]
  def main(args: Array[String]): scala.Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(1);
    val pref = args(0) + "/"

    PojoTypeInfo.
			registerCustomSerializer(classOf[ElementOrEvent[_]], new ElementOrEvent.ElementOrEventSerializerFactory)
    PojoTypeInfo.registerCustomSerializer(classOf[TupleIntInt], classOf[TupleIntInt.TupleIntIntSerializer])

    CFLConfig.getInstance.reuseInputs = args(2).toBoolean
    CFLConfig.getInstance.terminalBBId = 4

    val kickoffSrc = new KickoffSource(0, 1)
    env.addSource(kickoffSrc).addSink(new DiscardingSink[labyrinth.util.Unit])
    val para = env.getParallelism

    // BB 0
    val pageAttributesStream = env.createInput[Tuple2[Long, Long]](
      new TupleCsvInputFormat[Tuple2[Long, Long]](new Path(pref + "in/pageAttributes.tsv"),
        "\n",
        "\t", typeInfoTupleLongLong)
    )
      .map(new MapFunction[Tuple2[Long, Long], TupleLongLong]() {
        @throws[Exception]
        override def map(value: Tuple2[Long, Long]): TupleLongLong = TupleLongLong.of(value.f0, value.f1)
      }).javaStream

    val pageAttributes = new LabySource[TupleLongLong](pageAttributesStream,
      0,
      TypeInformation.of(new TypeHint[ElementOrEvent[TupleLongLong]]() {})
    )

    @SuppressWarnings(Array("unchecked")) val yesterdayCounts_1 = new LabySource[TupleLongLong](
      env.fromCollection[TupleLongLong](Seq()).javaStream,
      0,
      TypeInformation.of(new TypeHint[ElementOrEvent[TupleLongLong]]() {})
    )

    val day_1 = new LabySource[java.lang.Long](
      env.fromCollection(List[java.lang.Long](1L)).javaStream,
      0,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .setParallelism(1)

    // -- Iteration starts here --   BB 1
    val yesterdayCounts_2 = LabyNode.phi[TupleLongLong](
      "yesterdayCounts_2",
      1,
      new Forward[TupleLongLong](para),
      tupleLongLongSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[TupleLongLong]]() {})
    )
      .addInput(yesterdayCounts_1, false)

    val day_2 = LabyNode.phi[java.lang.Long](
      "day_2",
      1,
      new Always0[java.lang.Long](1),
      longSer, TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .addInput(day_1, false)
      .setParallelism(1)

    //import scala.collection.JavaConversions._
    val visits_1 = new LabyNode[java.lang.Long, java.lang.Long](
      "visits_1",
      new ClickLogReader(pref + "in/clickLog_"),
      1,
      new RoundRobin[java.lang.Long](para),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .addInput(day_2, true, false)

    //.setParallelism(1);

    // The inputs of the join have to be the same type (because of the union stuff), so we add a dummy tuple element.
    val visits_1_tupleized = new LabyNode[java.lang.Long, TupleLongLong](
      "visits_1_tupleized",
      ScalaOps.map((e: java.lang.Long) => new TupleLongLong(e, -1L)),
      1,
      new LongBy0(para),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[TupleLongLong]]() {})
    )
      .addInput(visits_1, true, false)

    val clicksJoined = new LabyNode[TupleLongLong, Tuple2[TupleLongLong, TupleLongLong]](
      "preJoinedWithAttrs",
      ScalaOps.joinGeneric(e => e.f0),
      1,
      new TupleLongLongBy0(para),
      tupleLongLongSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[Tuple2[TupleLongLong, TupleLongLong]]]() {})
    )
      .addInput(pageAttributes, false)
      .addInput(visits_1_tupleized, true, false)

    val clicksMapped = new LabyNode[Tuple2[TupleLongLong, TupleLongLong], TupleLongLong] (
      "joinedWithAttrs",
      ScalaOps.flatMap[Tuple2[TupleLongLong, TupleLongLong], TupleLongLong] (
        (t, out) => { if (t.f0.f1 == 0) { out.collectElement(new TupleLongLong(t.f1.f0, 1)) } }
      ),
      1,
      new Always0[Tuple2[TupleLongLong, TupleLongLong]](para),
      tuple2LongLongSerializer,
      TypeInformation.of(new TypeHint[ElementOrEvent[TupleLongLong]]() {})
    )
      .addInput(clicksJoined, true, false)

    val counts = new LabyNode[TupleLongLong, TupleLongLong](
      "counts",
      ScalaOps.reduceGroup( (t:TupleLongLong) => t.f0, (a,b) => new TupleLongLong(a.f0, a.f1 + b.f1) ),
      1,
      new TupleLongLongBy0(para),
      tupleLongLongSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[TupleLongLong]]() {})
    )
      .addInput(clicksMapped, true, false)

    val notFirstDay = new LabyNode[java.lang.Long, java.lang.Boolean](
      "notFirstDay",
      ScalaOps.singletonBagOperator(e => !(e==1)),
      1,
      new Always0[java.lang.Long](1),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Boolean]]() {})
    )
      .addInput(day_2, true, false)
      .setParallelism(1)

    val ifCond = new LabyNode[java.lang.Boolean, labyrinth.util.Unit](
      "ifCond",
      new ConditionNode(Array[Int](2, 3), Array[Int](3)),
      1,
      new Always0[java.lang.Boolean](1),
      booleanSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[labyrinth.util.Unit]]() {})
    )
      .addInput(notFirstDay, true, false)
      .setParallelism(1)


    // TODO
    // -- then branch   BB 2
    // The join of joinedYesterday is merged into this operator
    val diffs = new LabyNode[TupleLongLong, java.lang.Long](
      "diffs",
      new OuterJoinTupleLongLong[java.lang.Long]() {
        override protected def inner(b: Long, p: TupleLongLong): scala.Unit =
          out.collectElement(Math.abs(b - p.f1))

        override protected def right(p: TupleLongLong): scala.Unit = out.collectElement(p.f1)

        override protected def left(b: Long): scala.Unit = out.collectElement(b)},
      2,
      new TupleLongLongBy0(para),
      tupleLongLongSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .addInput(yesterdayCounts_2, false, true)
      .addInput(counts, false, true)

    val sumCombiner = new LabyNode[java.lang.Long, java.lang.Long](
      "sumCombiner",
      new SumCombinerLong,
      2,
      new Forward[java.lang.Long](para),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .addInput(diffs, true, false)

    val sum = new LabyNode[java.lang.Long, java.lang.Long](
      "sum",
      new SumLong,
      2,
      new Always0[java.lang.Long](1),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .addInput(sumCombiner, true, false)
      .setParallelism(1)

    val printSum = new LabyNode[java.lang.Long, labyrinth.util.Unit]("printSum",
      new CFAwareFileSink(pref + "out_scala/diff_"),
      2,
      new Always0[java.lang.Long](1),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[labyrinth.util.Unit]]() {})
    )
      .addInput(day_2, false, true)
      .addInput(sum, true, false).setParallelism(1)

    // -- end of then branch   BB 3
    // (We "optimize away" yesterdayCounts_3, since it would be an IdMap)
    yesterdayCounts_2.addInput(counts, false, true)

    val day_3 = new LabyNode[java.lang.Long, java.lang.Long](
      "day_3",
      new IncMapLong,
      3,
      new Always0[java.lang.Long](1),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Long]]() {})
    )
      .addInput(day_2, false, false)
      .setParallelism(1)

    day_2.addInput(day_3, false, true)

    val notLastDay = new LabyNode[java.lang.Long, java.lang.Boolean](
      "notLastDay",
      new SmallerThanLong(args(1).toLong + 1),
      3,
      new Always0[java.lang.Long](1),
      longSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[java.lang.Boolean]]() {})
    )
      .addInput(day_3, true, false)
      .setParallelism(1)

    val exitCond = new LabyNode[java.lang.Boolean, labyrinth.util.Unit](
      "exitCond",
      new ConditionNode(1, 4),
      3,
      new Always0[java.lang.Boolean](1),
      booleanSer,
      TypeInformation.of(new TypeHint[ElementOrEvent[labyrinth.util.Unit]]() {})
    )
      .addInput(notLastDay, true, false)
      .setParallelism(1)

    // -- Iteration ends here   BB 4
    // Itt nincs semmi operator. (A kiirast a BB 2-ben csinaljuk.)
    LabyNode.translateAll(env.getJavaEnv)

    env.execute
  }
}
