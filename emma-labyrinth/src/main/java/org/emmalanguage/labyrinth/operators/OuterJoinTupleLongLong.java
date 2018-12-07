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

import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.emmalanguage.labyrinth.util.SerializedBuffer;
import org.emmalanguage.labyrinth.util.TupleIntInt;
import org.emmalanguage.labyrinth.util.TupleLongLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Joins (key,b) (build) with (key,c) (probe), giving (b, key, c) to the udf.
 * The first input is the build side.
 */
public abstract class OuterJoinTupleLongLong<T> extends BagOperator<TupleLongLong, T> implements ReusingBagOperator {

    //private static final Logger LOG = LoggerFactory.getLogger(OuterJoinTupleLongLong.class);

    //private Int2ObjectOpenHashMap<HashMapElem> ht;
    private Long2ObjectRBTreeMap<HashMapElem> ht;
    private SerializedBuffer<TupleLongLong> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private boolean reuse = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        probeBuffered = new SerializedBuffer<>(new TupleLongLong.TupleLongLongSerializer());
        buildDone = false;
        probeDone = false;
        reuse = false;
    }

    @Override
    public void signalReuse() {
        reuse = true;
    }

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);

        if (logicalInputId == 0) {
            // build side
            if (!reuse) {
                //ht = new Int2ObjectOpenHashMap<>(1024, Int2ObjectOpenHashMap.VERY_FAST_LOAD_FACTOR);
                ht = new Long2ObjectRBTreeMap<>();
            }
        }
    }

    @Override
    public void pushInElement(TupleLongLong e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            assert !buildDone;
            HashMapElem l = ht.get(e.f0);
            if (l == null) {
                l = new HashMapElem(new LongArrayList(2));
                l.elems.add(e.f1);
                ht.put(e.f0,l);
            } else {
                l.elems.add(e.f1);
            }
        } else { // probe side
            if (!buildDone) {
                probeBuffered.add(e);
            } else {
                probe(e);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (inputId == 0) { // build side
            assert !buildDone;
            //LOG.info("Build side finished");
            buildDone = true;
            for (TupleLongLong e: probeBuffered) {
                probe(e);
            }
            if (probeDone) {
                doRight();
                out.closeBag();
            }
        } else { // probe side
            assert inputId == 1;
            assert !probeDone;
            //LOG.info("Probe side finished");
            probeDone = true;
            if (buildDone) {
                doRight();
                out.closeBag();
            }
        }
    }

    private void probe(TupleLongLong p) {
        HashMapElem l = ht.get(p.f0);
        if (l != null) {
            l.seen = true;
            // found the key in the hashtable
            for (long b: l.elems) {
                inner(b, p);
            }
        } else {
            // not found
            right(p);
        }
    }

    // Iterate on the build side and call left on not seen elements.
    private void doRight() {
        for (HashMapElem e: ht.values()) {
            if (!e.seen) {
                for (long b: e.elems) {
                    left(b);
                }
            }
        }
    }

    // b is the `value' in the build-side, p is the whole probe-side record (the key is p.f0)
    protected abstract void inner(long b, TupleLongLong p); // Uses out

    // p is the whole probe-side record
    protected abstract void right(TupleLongLong p); // Uses out

    // b is the value of one element.
    // (the key is not included, doRight would have to be modified to use int2ObjectEntrySet for that)
    protected abstract void left(long b); // Uses out


    private static final class HashMapElem {

        boolean seen;
        final LongArrayList elems;

        HashMapElem(LongArrayList elems) {
            this.seen = false;
            this.elems = elems;
        }
    }
}
