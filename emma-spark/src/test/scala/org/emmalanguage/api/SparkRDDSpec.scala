package org.emmalanguage
package api

import org.apache.spark.SparkContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


class SparkRDDSpec extends DataBagSpec {

  override type Bag[A] = SparkRDD[A]
  override type BackendContext = SparkContext

  override def withBackendContext[T](f: BackendContext => T): T =
    LocalSparkSession.withSparkContext(f)

  override def Bag[A: Meta : ClassTag : TypeTag](implicit sc: BackendContext): Bag[A] =
    SparkRDD[A]

  override def Bag[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit sc: BackendContext): Bag[A] =
    SparkRDD(seq)
}
