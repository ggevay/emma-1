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

import io.csv.{CSV, CSVConverter}

import org.apache.spark.sql.SparkSession

class SparkRDDSpec extends DataBagSpec {

  override type Bag[A] = SparkRDD[A]
  override type BackendContext = SparkSession

  override val suffix = "spark-rdd"

  override def withBackendContext[T](f: BackendContext => T): T =
    LocalSparkSession.withSparkSession(f)

  override def Bag[A: Meta]()(implicit spark: BackendContext): Bag[A] =
    SparkRDD[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit spark: BackendContext): Bag[A] =
    SparkRDD(seq)

  override def readCSV[A : Meta : CSVConverter](path: String, format: CSV)(implicit ctx: BackendContext): DataBag[A] =
    SparkRDD.readCSV[A](path, format)
}
