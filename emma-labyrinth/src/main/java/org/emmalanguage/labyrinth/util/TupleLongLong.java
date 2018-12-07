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

package org.emmalanguage.labyrinth.util;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public final class TupleLongLong implements Serializable {

    public long f0, f1;

    public TupleLongLong() {}

    public TupleLongLong(long f0, long f1) {
        this.f0 = f0;
        this.f1 = f1;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleLongLong that = (TupleLongLong) o;
        return f0 == that.f0 &&
                f1 == that.f1;
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1);
    }

    @Override
    public String toString() {
        return "TupleLongLong{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                '}';
    }

    public static TupleLongLong of(long f0, long f1) {
        return new TupleLongLong(f0, f1);
    }


    // ------------------------- Serializers -------------------------

    public static final class TupleLongLongSerializer extends TypeSerializer<TupleLongLong> {

        @Override
        public TypeSerializerConfigSnapshot snapshotConfiguration() {
            return null;
        }

        @Override
        public CompatibilityResult<TupleLongLong> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
            return null;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TupleLongLong> duplicate() {
            return this;
        }

        @Override
        public TupleLongLong createInstance() {
            return new TupleLongLong();
        }

        @Override
        public TupleLongLong copy(TupleLongLong from) {
            return copy(from, new TupleLongLong());
        }

        @Override
        public TupleLongLong copy(TupleLongLong from, TupleLongLong reuse) {
            reuse.f0 = from.f0;
            reuse.f1 = from.f1;
            return reuse;
        }

        @Override
        public int getLength() {
            return 16;
        }

        @Override
        public void serialize(TupleLongLong record, DataOutputView target) throws IOException {
            target.writeLong(record.f0);
            target.writeLong(record.f1);
        }

        @Override
        public TupleLongLong deserialize(DataInputView source) throws IOException {
            return deserialize(createInstance(), source);
        }

        @Override
        public TupleLongLong deserialize(TupleLongLong reuse, DataInputView source) throws IOException {
            reuse.f0 = source.readLong();
            reuse.f1 = source.readLong();
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.write(source, getLength());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TupleLongLongSerializer;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof TupleLongLongSerializer;
        }

        @Override
        public int hashCode() {
            return 45;
        }
    }
}
