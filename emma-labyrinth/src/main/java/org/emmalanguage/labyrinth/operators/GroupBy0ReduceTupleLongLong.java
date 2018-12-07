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

package org.emmalanguage.labyrinth.operators;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import org.emmalanguage.labyrinth.util.TupleIntInt;
import org.emmalanguage.labyrinth.util.TupleLongLong;

import java.util.function.Consumer;

//import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public abstract class GroupBy0ReduceTupleLongLong extends BagOperator<TupleLongLong, TupleLongLong> {

    //protected Long2LongOpenHashMap hm;
    protected Long2LongRBTreeMap hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        //hm = new Int2IntOpenHashMap(1024, Int2IntOpenHashMap.VERY_FAST_LOAD_FACTOR);
        hm = new Long2LongRBTreeMap();
        hm.defaultReturnValue(Integer.MIN_VALUE);
    }

    @Override
    public void pushInElement(TupleLongLong e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        long g = hm.putIfAbsent(e.f0, e.f1);
        if (g != hm.defaultReturnValue()) {
            reduceFunc(e, g);
        }
    }

    protected abstract void reduceFunc(TupleLongLong e, long g);

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        //hm.int2IntEntrySet().fastForEach(new Consumer<Int2IntMap.Entry>() {
        hm.long2LongEntrySet().forEach(new Consumer<Long2LongMap.Entry>() {
            @Override
            public void accept(Long2LongMap.Entry e) {
                out.collectElement(TupleLongLong.of(e.getLongKey(), e.getLongValue()));
            }
        });

        hm = null;

        out.closeBag();
    }
}
