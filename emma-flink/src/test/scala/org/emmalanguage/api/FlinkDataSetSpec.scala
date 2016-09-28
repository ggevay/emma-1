package org.emmalanguage
package api

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.TypeUtils

class FlinkDataSetSpec extends DataBagSpec {
  override type Bag[A] = FlinkDataSet[A]
  override type BackendContext = ExecutionEnvironment

  override def withBackendContext[T](f: BackendContext => T): T =
    f(ExecutionEnvironment.getExecutionEnvironment)

  override def Bag[A: Meta : ClassTag : TypeTag](implicit flink: ExecutionEnvironment): Bag[A] =
    FlinkDataSet[A]

  override def Bag[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit flink: ExecutionEnvironment): Bag[A] =
    FlinkDataSet(seq)


  "aa" in {
    import eu.stratosphere.emma.testschema.Literature._

    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

//    org.apache.flink.api.scala.createTypeInformation[Int]
//    org.apache.flink.api.scala.createTypeInformation[Character]
    //FlinkDataSet.typeInformationForType[Character]


//    val act = env.fromCollection(hhCrts)
//      .map(c => c.name)
//
//    val exp = hhCrts
//      .map(c => c.name)
//
//    act.collect() shouldEqual exp


//
//    org.apache.flink.api.scala.createTypeInformation[Int]
//
    FlinkDataSet.typeInformationForType[Int]
//    FlinkDataSet.typeInformationForType[Book]
//    FlinkDataSet.typeInformationForType[Character]
  }
}