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

package org.emmalanguage.labyrinth.partitioners;

/**
 * Partition a bag of Integers by the whole element.
 */
public class LongBy0 extends Partitioner<Long> {

    public LongBy0(int targetPara) {
        super(targetPara);
    }

    @Override
    public short getPart(Long elem, short subpartitionId) {
        return (short)(fix(elem) % targetPara);
    }

    public static long fix(long l) {
        if (l < 0) {
            return Math.abs(l + 1);
        } else {
            return l;
        }
    }
}
