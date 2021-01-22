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

package org.emmalanguage.mitos.partitioners;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Partition a bag of Tuple2s by f0.
 */
public class Tuple2by0<T> extends Partitioner<Tuple2<Integer, T>> {

    public Tuple2by0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(Tuple2<Integer, T> elem, short subpartitionId) {
        return (short)(elem.f0 % targetPara);
    }
}
