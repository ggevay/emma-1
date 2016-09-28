package org.emmalanguage
package api

import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


class SparkDatasetSpec extends DataBagSpec {

  override type Bag[A] = SparkDataset[A]
  override type BackendContext = SparkSession

  override def withBackendContext[T](f: BackendContext => T): T =
    LocalSparkSession.withSparkSession(f)

  override def Bag[A: Meta : ClassTag : TypeTag](implicit spark: SparkSession): Bag[A] =
    SparkDataset[A]

  override def Bag[A: Meta : ClassTag : TypeTag](seq: Seq[A])(implicit spark: SparkSession): Bag[A] =
    SparkDataset(seq)
}
