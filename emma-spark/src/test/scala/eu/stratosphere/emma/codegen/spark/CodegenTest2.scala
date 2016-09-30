package eu.stratosphere.emma
package codegen.spark

import codegen.BaseCodegenTest2
import runtime.Spark

class CodegenTest2 extends BaseCodegenTest2("spark") {

  override def runtimeUnderTest: Spark =
    Spark.testing()
}
