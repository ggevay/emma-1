package org.emmalanguage.api

import org.apache.flink.api.common.typeinfo.TypeInformation

class FlinkDataSet {

}

object FlinkDataSet {

  def typeInfo[A: TypeInformation]: TypeInformation[A] =
    implicitly[TypeInformation[A]]
}
