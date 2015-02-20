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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.Outputs;

import java.util.*;

public class BinaryScoreOutputs extends ScoreOutputs<BytesRef> {


    private final static BytesRef INFINITY = new BytesRef();

    public BinaryScoreOutputs(Outputs<BytesRef> outputs) {
        super(outputs);
    }

    private ScoreOutput<BytesRef> mapCommon(ScoreOutput<BytesRef> fromOutput, ScoreOutput<BytesRef> toOutput) {
        List<BytesRef> common = new ArrayList<>();
        List<Integer> mapping = new ArrayList<>();
        // get the best common of the two outputs and save the mappings
        for (int i = 0; i < fromOutput.size(); i++) {
            BytesRef out1 = fromOutput.outputs.get(i);
            BytesRef bestCommon = null;
            int bestMapping = -1;

            for (int j = 0; j < toOutput.size(); j++) {
                BytesRef out2 = toOutput.outputs.get(j);
                if (bestCommon == null) {
                    bestCommon = outputs.common(out1, out2);
                    bestMapping = j;
                } else {
                    BytesRef temp = outputs.common(out1, out2);
                    int tempSize = temp.length - temp.offset;
                    int bestCommonSize = bestCommon.length - bestCommon.offset;
                    if (tempSize > bestCommonSize) {
                        bestCommon = temp;
                        bestMapping = j;
                    }
                }
            }
            common.add(bestCommon);
            mapping.add(bestMapping);
        }
        // check for left over from toOutput
        // add left over with a mapping of -1
        Set<Integer> mappedToOutput = new HashSet<>(mapping);
        if (mappedToOutput.size() < toOutput.size()) {
            for (int i = 0; i < toOutput.size(); i++) {
                if (!mappedToOutput.contains(i)) {
                    common.add(toOutput.outputs.get(i));
                    mapping.add(-1);
                }
            }
        }

        return new ScoreOutput<>(new ArrayList<>(common), mapping.toArray(new Integer[mapping.size()]));
    }

    @Override   // this, last outputs
    public ScoreOutput<BytesRef> common(ScoreOutput<BytesRef> output1, ScoreOutput<BytesRef> output2) {
        System.out.println("In Common");
        System.out.println("output1: " + outputToString(output1));
        System.out.println("output1: " + outputToString(output2));
        System.out.println("Result: " + outputToString(mapCommon(output1, output2)));
        //if (output1.size() >= output2.size()) {
            return mapCommon(output1, output2);
        //} else {
        //    return mapCommon(output2, output1);
        //}
    }

    @Override                           // last outputs, commons
    public ScoreOutput<BytesRef> subtract(ScoreOutput<BytesRef> output, ScoreOutput<BytesRef> inc) {

        System.out.println("In subtract");
        System.out.println("output1: " + outputToString(output));
        System.out.println("output1: " + outputToString(inc));
        List<BytesRef> substract = new ArrayList<>();
        for (int i = 0; i < inc.size(); i++) {
            BytesRef commonOut = inc.outputs.get(i);
            int mapping = inc.mapping[i];
            //TODO: CHANGE
            if (mapping >= output.size()) {
                int xx = 0;
            }
            assert mapping < output.size();
            if (mapping == -1) {
                substract.add(INFINITY);
            } else {
                substract.add(outputs.subtract(output.outputs.get(mapping), commonOut));
            }
        }
        ScoreOutput<BytesRef> bytesRefs = new ScoreOutput<>(substract, inc.mapping);
        System.out.println("Result: " + outputToString(bytesRefs));
        return bytesRefs;
    }

    @Override    // subtracts, other node arcs
    public ScoreOutput<BytesRef> add(ScoreOutput<BytesRef> prefix, ScoreOutput<BytesRef> output) {
        System.out.println("In add");
        System.out.println("output1: " + outputToString(prefix));
        System.out.println("output1: " + outputToString(output));
        if (output == NO_OUTPUT) {
            return prefix;
        } else if (prefix == NO_OUTPUT) {
            return output;
        }
        List<BytesRef> add = new ArrayList<>();
        for (int i = 0; i < prefix.size(); i++) {
            BytesRef prefixOut = prefix.outputs.get(i);
            int mapping = prefix.mapping[i];
            //TODO: CHANGE
            if (mapping >= output.size()) {
                int xx = 0;
            }
            if (mapping != -1) {
                add.add(outputs.add(prefixOut, output.outputs.get(mapping)));
            }
        }
        ScoreOutput<BytesRef> bytesRefs = new ScoreOutput<>(add, prefix.mapping);
        System.out.println("Result: " + outputToString(bytesRefs));
        return bytesRefs;
    }

}
