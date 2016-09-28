package org.emmalanguage.api

import eu.stratosphere.emma.testschema.Literature.Book

import org.apache.flink.api.scala.ExecutionEnvironment

class FlinkDataSetSpec extends DataBagSpec {

  override type Bag[A] = FlinkDataSet[A]
  override type BackendContext = ExecutionEnvironment

  override def withBackendContext[T](f: BackendContext => T): T =
    f(ExecutionEnvironment.getExecutionEnvironment)

  override def Bag[A: Meta](implicit flink: ExecutionEnvironment): Bag[A] =
    FlinkDataSet[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit flink: ExecutionEnvironment): Bag[A] =
    FlinkDataSet(seq)

  "playground" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

//    implicit val objMeta = typeMeta[(Int, String)]
//
//    println("foo")
//    env.fromCollection(Seq((1, "foo"), (2, "bar"), (3, "baz"))).map(x => x._1 * 5)
//    println("bar")

  }
}
