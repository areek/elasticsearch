/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.Outputs;

import java.io.IOException;
import java.util.*;

/**
 * get common(this, last output)
 * subtract(last output, common)
 * add(prev_node, substract)
 */

public abstract class ScoreOutputs<T> extends Outputs<ScoreOutputs.ScoreOutput<T>> {

    protected final Outputs<T> outputs;

    public static ScoreOutput NO_OUTPUT = new ScoreOutput(null);

    protected ScoreOutputs(Outputs<T> outputs) {
        this.outputs = outputs;
    }

    public static class ScoreOutput<T> implements Iterable<T> {
        Integer[] mapping;
        List<T> outputs;
        List<BytesRef> payloads;

        ScoreOutput(List<T> outputs) {
            this.outputs = outputs;
            this.mapping = null;
        }

        ScoreOutput(Collection<Entry<T>> entries) {
            this.outputs = new ArrayList<>(entries.size());
            this.payloads = new ArrayList<>(entries.size());
            this.mapping = new Integer[entries.size()];
            int count = 0;
            for (Entry<T> entry : entries) {
                outputs.add(entry.weight());
                payloads.add(entry.payload());
                mapping[count] = count;
                count++;

            }
        }

        ScoreOutput(List<T> outputs, Integer[] mapping) {
            this.outputs = outputs;
            this.mapping = mapping;
        }

        ScoreOutput(List<T> outputs, List<BytesRef> payloads, Integer[] mapping) {
            this.outputs = outputs;
            this.mapping = mapping;
            this.payloads = payloads;
        }

        public void activateOutputs(int... indexes) {
            for (int index : indexes) {
                assert index >= 0 && index < outputs.size();
            }
            //this.activeOutputs = indexes;
        }

        public int[] activeOutputs() {
            //TODO
            return new int[0];
        }

        public T weight(int index) {
            assert index >= 0 && index < outputs.size();
            return this.outputs.get(index);
        }

        public BytesRef payload(int index) {
            assert index >= 0 && index < outputs.size();
            return this.payloads.get(index);
        }

        int size() {
            if (outputs == null) {
                return 0;
            }
            return outputs.size();
        }

        @Override
        public Iterator<T> iterator() {
            return outputs.iterator();
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ScoreOutput that = (ScoreOutput) o;

            if (!Arrays.equals(mapping, that.mapping)) return false;
            if (outputs != null ? !outputs.equals(that.outputs) : that.outputs != null) return false;
            if (payloads != null ? !payloads.equals(that.payloads) : that.payloads != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = mapping != null ? Arrays.hashCode(mapping) : 0;
            result = 31 * result + (outputs != null ? outputs.hashCode() : 0);
            result = 31 * result + (payloads != null ? payloads.hashCode() : 0);
            return result;
        }
    }

    public static interface Entry<T> {
        public BytesRef payload();
        public T weight();
    }

    public static <T> ScoreOutput<T> fromEntries(Collection<Entry<T>> entries) {
        return new ScoreOutput<>(entries);
    }

    @Override
    public void write(ScoreOutput<T> output, DataOutput out) throws IOException {
        assert output != null;
        if (output.mapping == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(output.mapping.length);
            for (int i = 0; i < output.mapping.length; i++) {
                out.writeVInt(output.mapping[i] + 1);
            }
        }
        out.writeVInt(output.size());
        for (T entry : output) {
            outputs.write(entry, out);
        }

        if (output.payloads != null) {
            int payloadSize = output.payloads.size();
            out.writeVInt(payloadSize);
            for (BytesRef payload : output.payloads) {
                out.writeVInt(payload.length);
                out.writeBytes(payload.bytes, payload.offset, payload.length);
            }
        } else {
            out.writeVInt(0);
        }
    }


    @Override
    public ScoreOutput<T> read(DataInput in) throws IOException {
        int mappingSize = in.readVInt();
        Integer[] mappings;
        if (mappingSize == 0) {
            mappings = null;
        } else {
            mappings = new Integer[mappingSize];
            for (int i = 0; i < mappingSize; i++) {
                mappings[i] = in.readVInt() - 1;
            }
        }
        int entrySize = in.readVInt();
        List<T> entries = new ArrayList<>(entrySize);
        for (int i = 0; i < entrySize; i++) {
            entries.add(outputs.read(in));
        }
        final int len = in.readVInt();
        List<BytesRef> payloads;
        if (len == 0) {
            payloads = null;
        } else {
            payloads = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                int payloadLen = in.readVInt();
                final BytesRef output = new BytesRef(payloadLen);
                in.readBytes(output.bytes, 0, payloadLen);
                output.length = payloadLen;
                payloads.add(output);
            }
        }

        return new ScoreOutput<>(entries, payloads, mappings);
    }

    @Override
    public ScoreOutput<T> getNoOutput() {
        return NO_OUTPUT;
    }

    @Override
    public String outputToString(ScoreOutput<T> output) {
        assert output != null;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < output.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("(");
            sb.append(outputs.outputToString(output.outputs.get(i)));
            if (output.mapping != null) {
                sb.append(", ");
                sb.append(output.mapping[i]);
            }
            sb.append(")");
        }
        sb.append("]");
        return sb.toString();
    }

    public long ramBytesUsed(ScoreOutput<T> output) {
        long ramBytesUsed = 0;
        if (output.mapping != null) {
            ramBytesUsed = RamUsageEstimator.shallowSizeOf(output.mapping);
            ramBytesUsed += output.mapping.length * RamUsageEstimator.NUM_BYTES_INT;
        }
        for (T out : output) {
            ramBytesUsed += outputs.ramBytesUsed(out);
        }
        return ramBytesUsed;
    }
}
