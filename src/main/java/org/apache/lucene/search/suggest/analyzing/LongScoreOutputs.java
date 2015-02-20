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

import org.apache.lucene.util.fst.Outputs;

import java.util.ArrayList;
import java.util.List;

public class LongScoreOutputs extends ScoreOutputs<Long> {

    protected LongScoreOutputs(Outputs<Long> outputs) {
        super(outputs);
    }

    @Override //this, last outputs
    public ScoreOutput<Long> common(ScoreOutput<Long> output1, ScoreOutput<Long> output2) {
        System.out.println("In Common");
        System.out.println("output1: " + outputToString(output1));
        System.out.println("output1: " + outputToString(output2));
        int totalSize = Math.max(output1.size(), output2.size());
        Integer[] mappings = new Integer[totalSize];
        List<Long> common = new ArrayList<>(totalSize);
        int minimumSize = Math.min(output1.size(), output2.size());
        int count = 0;
        while (count < minimumSize) {
            common.add(outputs.common(output1.weight(count), output2.weight(count)));
            mappings[count] = count;
            count++;
        }
        if (minimumSize == output1.size()) {
            for (int i = minimumSize; i < output2.size(); i++) {
                common.add(output2.weight(i));
                mappings[i] = i;
            }
        } else {
            for (int i = minimumSize; i < output1.size(); i++) {
                common.add(output1.weight(i));
                mappings[i] = i;
            }
        }
        ScoreOutput<Long> longs = new ScoreOutput<>(common, mappings);
        System.out.println("Result: " + outputToString(longs));
        return longs;
    }

    @Override // last outputs, commons
    public ScoreOutput<Long> subtract(ScoreOutput<Long> output, ScoreOutput<Long> inc) {

        System.out.println("In subtract");
        System.out.println("output1: " + outputToString(output));
        System.out.println("output1: " + outputToString(inc));

        List<Long> subtract = new ArrayList<>();
        List<Integer> mappings = new ArrayList<>();
        /*
        int minimumSize = Math.min(output.size(), inc.size());
        int count = 0;
        for (int i = 0; i < inc.size(); i++) {
            int index = inc.mapping[i];
            if (index >= 0) {
                assert index < output.size();
                subtract.add(outputs.subtract(output.weight(index), inc.weight(i)));
                mappings.add(index);
            }
        }*/

        for (int i = 0; i < output.size(); i++) {
            int index = output.mapping[i];
            if (index >= 0) {
                subtract.add(outputs.subtract(output.weight(index), inc.weight(i)));
                mappings.add(index);
            }
        }

        if (output.size() < inc.size()) {
            for (int i = output.size(); i < inc.size(); i++) {
                subtract.add(inc.weight(i));
                mappings.add(inc.mapping[i]);
            }
        }
        /*
        while (count < minimumSize) {
            subtract.add(outputs.subtract(output.weight(count), inc.weight(count)));
            count++;
        }
        if (minimumSize == output.size()) {
            for (int i = minimumSize; i < inc.size(); i++) {
                subtract.add(inc.weight(i));
            }
        } else {
            for (int i = minimumSize; i < output.size(); i++) {
                subtract.add(output.weight(i));
            }
        }
        */

        ScoreOutput<Long> longs = new ScoreOutput<>(subtract, mappings.toArray(new Integer[mappings.size()]));
        System.out.println("Result: " + outputToString(longs));
        return longs;
    }

    @Override // subtracts, other node arcs
    public ScoreOutput<Long> add(ScoreOutput<Long> prefix, ScoreOutput<Long> output) {
        if (prefix == NO_OUTPUT) {
            return output;
        } else if (output == NO_OUTPUT) {
            return prefix;
        }
        System.out.println("In add");
        System.out.println("output1: " + outputToString(prefix));
        System.out.println("output1: " + outputToString(output));
        List<Long> add = new ArrayList<>();
        List<Integer> mappings = new ArrayList<>();
        int minimumSize = Math.min(prefix.size(), output.size());
        int count = 0;
        while (count < minimumSize) {
            add.add(outputs.add(prefix.weight(count), output.weight(count)));
            mappings.add(count);
            count++;
        }
        if (minimumSize == prefix.size()) {
            for (int i = minimumSize; i < output.size(); i++) {
                add.add(output.weight(i));
                mappings.add(i);
            }
        } else {
            for (int i = minimumSize; i < prefix.size(); i++) {
                add.add(prefix.weight(i));
                mappings.add(i);
            }
        }

        ScoreOutput<Long> longs = new ScoreOutput<>(add, mappings.toArray(new Integer[mappings.size()]));
        System.out.println("Result: " + outputToString(longs));
        return longs;
    }
}
