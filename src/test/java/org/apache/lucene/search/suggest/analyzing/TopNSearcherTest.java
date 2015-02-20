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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.*;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class TopNSearcherTest extends LuceneTestCase {

    //SCRATCH

    @Test
    public void scratchlong() throws Exception {

        List<Entry<Long>> inputs = new ArrayList<>();
        inputs.add(new Entry<>("term1", 1l, 1));
        inputs.add(new Entry<>("term1", 2l, 2));
        inputs.add(new Entry<>("term1", 3l, 3));
        inputs.add(new Entry<>("te2", 4l, 4));
        inputs.add(new Entry<>("te2", 5l, 5));
        inputs.add(new Entry<>("tem2", 6l, 6));
        AbstractMap.SimpleEntry<Integer, FST<ScoreOutputs.ScoreOutput<Long>>> build = build(longOutputConfiguration, inputs.toArray(new Entry[inputs.size()]));

        try {
            PrintWriter pw = new PrintWriter("/tmp/out.dot");
            Util.toDot(build.getValue(), pw, true, true);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        TestTopNSearcher<Long> longTestTopNSearcher = new TestTopNSearcher<>(build.getValue(), 3, 3);

        Map<BytesRef, List<Entry>> result = query(longTestTopNSearcher, "tem");
        for (Map.Entry<BytesRef, List<Entry>> bytesRefListEntry : result.entrySet()) {
            System.out.println(bytesRefListEntry.getKey().utf8ToString());
            for (Entry entry : bytesRefListEntry.getValue()) {
                System.out.println(" >> (" + entry.term + ", " + entry.weight + ", " + entry.docID+ ")");
            }
        }

    }

    @Test
    public void scratch() throws Exception {

        List<Entry<BytesRef>> inputs = new ArrayList<>();
        inputs.add(new Entry<>("term1", new BytesRef("abcdef"), 1));
        inputs.add(new Entry<>("term1", new BytesRef("abdeef"), 2));
        inputs.add(new Entry<>("term1", new BytesRef("badefg"), 3));
        inputs.add(new Entry<>("te2", new BytesRef("abcdef"), 4));
        inputs.add(new Entry<>("te2", new BytesRef("abdeef"), 5));
        inputs.add(new Entry<>("tem2", new BytesRef("badefg"), 6));
        AbstractMap.SimpleEntry<Integer, FST<ScoreOutputs.ScoreOutput<BytesRef>>> build = build(bytesRefOutputConfiguration, inputs.toArray(new Entry[inputs.size()]));

        try {
            PrintWriter pw = new PrintWriter("/tmp/out.dot");
            Util.toDot(build.getValue(), pw, true, true);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    final static ScoreOutputs<BytesRef> binaryScoreOutputs = new BinaryScoreOutputs(ByteSequenceOutputs.getSingleton());
    ContextAwareScorer.OutputConfiguration<BytesRef> bytesRefOutputConfiguration = new ContextAwareScorer.OutputConfiguration<BytesRef>() {
        @Override
        public ScoreOutputs<BytesRef> outputSingleton() {
            return binaryScoreOutputs;
        }

        @Override
        public BytesRef encode(BytesRef input) {
            return input;
        }

        @Override
        public BytesRef decode(BytesRef output) {
            return output;
        }
    };
    public void testSimple() throws Exception {
        Entry[] inputs = {new Entry("term1", 1l, 1), new Entry("term1", 2l, 2), new Entry("term2", 3l, 3)};
        AbstractMap.SimpleEntry<Integer, FST<ScoreOutputs.ScoreOutput<Long>>> build = build(longOutputConfiguration, inputs);
        TestTopNSearcher testTopNSearcher = new TestTopNSearcher(build.getValue(), 2, 2, build.getKey());
        Map<BytesRef, List<Entry>> result = query(testTopNSearcher, "term");
        assertResults(mapByTerm(inputs), result);
    }

    @Test
    public void testLeafPruning() throws Exception {
        int nterms = 10;
        int nLeaf = 10;
        Entry[] inputs = new Entry[nterms * nLeaf];
        for (int i = 0; i < nterms; i++) {
            String term = "term_" + String.valueOf(i);
            for (int l = 0; l < nLeaf; l++) {
                inputs[(i * nterms) + l] = new Entry(term, (i * nterms) + l, (i * nterms) + l);
            }
        }
        int pruneNFirstLeaves = 3;

        AbstractMap.SimpleEntry<Integer, FST<ScoreOutputs.ScoreOutput<Long>>> build = build(longOutputConfiguration, inputs);
        TestTopNSearcher testTopNSearcher = new TestTopNSearcher(build.getValue(), nterms, build.getKey(), pruneNFirstLeaves);
        Map<BytesRef, List<Entry>> result = query(testTopNSearcher, "term");
        Map<BytesRef, List<Entry>> stringListMap = mapByTerm(inputs);
        for (BytesRef term : stringListMap.keySet()) {
            List<Entry> entries = stringListMap.get(term);
            stringListMap.put(term, entries.subList(pruneNFirstLeaves, entries.size()));
        }
        assertResults(stringListMap, result);
    }

    @Test
    public void testFullTermSearch() throws Exception {
        Entry[] inputs = {new Entry("term1", 1l, 1), new Entry("term1", 2l, 2), new Entry("term1", 3l, 3)};
        AbstractMap.SimpleEntry<Integer, FST<ScoreOutputs.ScoreOutput<Long>>> build = build(longOutputConfiguration, inputs);
        TestTopNSearcher testTopNSearcher = new TestTopNSearcher(build.getValue(), 1, 3, build.getKey());
        Map<BytesRef, List<Entry>> result = query(testTopNSearcher, "term1");
        assertResults(mapByTerm(inputs), result);
    }

    private void assertResults(Map<BytesRef, List<Entry>> expected, Map<BytesRef, List<Entry>> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<BytesRef, List<Entry>> expectedEntrySet : expected.entrySet()) {
            List<Entry> actualEntries = actual.get(expectedEntrySet.getKey());
            assertNotNull(actualEntries);
            assertEquals(expectedEntrySet.getValue().size(), actualEntries.size());
            int i = 0;
            for (Entry expectedEntry : expectedEntrySet.getValue()) {
                Entry actualEntry = actualEntries.get(i);
                assertEquals(expectedEntry.term, actualEntry.term);
                assertEquals(expectedEntry.weight, actualEntry.weight);
                assertEquals(expectedEntry.docID, actualEntry.docID);
                i++;
            }
        }
    }

    private static <W> Map<BytesRef, List<Entry>> query(TestTopNSearcher<W> searcher, String term) throws IOException {
        Automaton automaton = Automata.makeString(term);
        //List<FSTUtil.Path<? extends ScoreOutputs.ScoreOutput<?>>> paths =
        List<FSTUtil.Path<ScoreOutputs.ScoreOutput<W>>> paths = FSTUtil.intersectPrefixPaths(automaton, searcher.fst);
        for (FSTUtil.Path<ScoreOutputs.ScoreOutput<W>> path : paths) {
            searcher.addStartPaths(path.fstNode, path.output, false, path.input);
        }
        searcher.search();
        return searcher.getResults();
    }

    final static ScoreOutputs<Long> longScoreOutputs = new LongScoreOutputs(PositiveIntOutputs.getSingleton());
    ContextAwareScorer.OutputConfiguration<Long> longOutputConfiguration = new ContextAwareScorer.OutputConfiguration<Long>() {
        @Override
        public ScoreOutputs<Long> outputSingleton() {
            return longScoreOutputs;
        }

        @Override
        public Long encode(Long input) {
            if (input < 0 || input > Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("cannot encode value: " + input);
            }
            return Integer.MAX_VALUE - input;
        }

        @Override
        public Long decode(Long output) {
            return (Integer.MAX_VALUE - output);
        }
    };

    private static class Entry<W> {
        private final String term;
        private final W weight;
        private final int docID;

        public Entry(String term, W weight, int docID) {
            this.term = term;
            this.weight = weight;
            this.docID = docID;
        }

    }

    private <W> Map<BytesRef, List<Entry<W>>> mapByTerm(Entry<W>... entries) {
        Map<BytesRef, List<Entry<W>>> entriesByTerm = new TreeMap<>(new Comparator<BytesRef>() {
            @Override
            public int compare(BytesRef o1, BytesRef o2) {
                return o1.compareTo(o2);
            }
        });
        for (Entry<W> entry : entries) {
            final List<Entry<W>> entryList;
            BytesRef entryTerm = new BytesRef(entry.term);
            if (!entriesByTerm.containsKey(entryTerm)) {
                entryList = new ArrayList<>();
                entriesByTerm.put(entryTerm, entryList);
            } else {
                entryList = entriesByTerm.get(entryTerm);
            }
            entryList.add(entry);
        }

            /* TODO
        for (Map.Entry<BytesRef, List<Entry>> stringListEntry : entriesByTerm.entrySet()) {

            Collections.sort(stringListEntry.getValue(), new Comparator<Entry>() {
                @Override
                public int compare(Entry o1, Entry o2) {
                    return Long.compare(o2.weight, o1.weight);
                }
            });
        }
            */
        return entriesByTerm;

    }

    private <W extends Comparable<W>> AbstractMap.SimpleEntry<Integer, FST<ScoreOutputs.ScoreOutput<W>>> build(ContextAwareScorer.OutputConfiguration<W> outputConfiguration, Entry... entries) throws IOException {
      NRTSuggesterBuilder.FSTBuilder<W> builder = new NRTSuggesterBuilder.FSTBuilder<>(outputConfiguration, NRTSuggesterBuilder.PAYLOAD_SEP, NRTSuggesterBuilder.END_BYTE);
        Map<BytesRef, List<Entry<W>>> entriesByTerm = mapByTerm(entries);
        int maxFormPerLeaf = 0;
        for (Map.Entry<BytesRef, List<Entry<W>>> termEntries : entriesByTerm.entrySet()) {
            BytesRef term = termEntries.getKey();
            builder.startTerm(term);
            for (Entry<W> entry : termEntries.getValue()) {
                builder.addEntry(entry.docID, term, entry.weight);
            }
            builder.finishTerm();
            maxFormPerLeaf = Math.max(maxFormPerLeaf, termEntries.getValue().size());
        }

        return new AbstractMap.SimpleEntry<>(maxFormPerLeaf, builder.build());
    }

    private class TestTopNSearcher<W> extends TopNSearcher<ScoreOutputs.ScoreOutput<W>> {

        private Map<BytesRef, List<Entry>> results = new HashMap<>();
        private List<Entry> currentResults = new ArrayList<>();

        private BytesRefBuilder currentKeyBuilder = new BytesRefBuilder();
        private CharsRefBuilder spare = new CharsRefBuilder();
        private final FST<ScoreOutputs.ScoreOutput<W>> fst;
        private final int pruneFirstNLeaves;
        private int leafCount;

        public TestTopNSearcher(FST<ScoreOutputs.ScoreOutput<W>> fst, int topN, int maxQueueDepth) {
            this(fst, topN, maxQueueDepth, 0);
        }
        public TestTopNSearcher(FST<ScoreOutputs.ScoreOutput<W>> fst, int topN, int maxQueueDepth, int pruneFirstNLeaves) {
            super(fst, topN, topN * maxQueueDepth, new Comparator<ScoreOutputs.ScoreOutput<W>>() {
                @Override
                public int compare(ScoreOutputs.ScoreOutput<W> o1, ScoreOutputs.ScoreOutput<W> o2) {
                    return 0;
                }
            });
            this.fst = fst;
            this.pruneFirstNLeaves = pruneFirstNLeaves;
        }

        //@Override
        //protected void onLeafNode(IntsRef input, PairOutputs.Pair<ScoreOutputs.ScoreOutput<Long>, BytesRef> output) {
        //    Util.toBytesRef(input, currentKeyBuilder);
        //    leafCount = 0;
        //}

        //@Override
        //protected boolean collectOutput(PairOutputs.Pair<ScoreOutputs.ScoreOutput<Long>, BytesRef> output) throws IOException {
        //    leafCount++;
        //    if (leafCount <= pruneFirstNLeaves) {
        /*        return false;
            }
            //TODO
            long weight = 0;//longOutputConfiguration.decode(output.output1);

            int surfaceFormLen = -1;
            for (int i = 0; i < output.output2.length; i++) {
                if (output.output2.bytes[output.output2.offset + i] == NRTSuggesterBuilder.PAYLOAD_SEP) {
                    surfaceFormLen = i;
                    break;
                }
            }
            assert surfaceFormLen != -1 : "no payloadSep found, unable to determine surface form";
            spare.copyUTF8Bytes(output.output2.bytes, output.output2.offset, surfaceFormLen);
            ByteArrayDataInput input = new ByteArrayDataInput(output.output2.bytes, surfaceFormLen + output.output2.offset + 1, output.output2.length - (surfaceFormLen + output.output2.offset));
            currentResults.add(new Entry(spare.toString(), weight, input.readVInt()));
            return true;
        }

        @Override
        protected void onFinishNode() throws IOException {
            results.put(currentKeyBuilder.toBytesRef(), new ArrayList<>(currentResults));
            currentResults.clear();
        }
*/
        public Map<BytesRef, List<Entry>> getResults() {
            return results;
        }

        @Override
        protected int acceptResults(IntsRef input, ScoreOutputs.ScoreOutput<W> output) {
            //TODO: IMPLEMENT
            Util.toBytesRef(input, currentKeyBuilder);

            for (int i = 0; i < output.size(); i++) {
                BytesRef payload = output.payload(i);
                W weight = output.weight(i);

                int surfaceFormLen = -1;
                for (int j = 0; j < payload.length; j++) {
                    if (payload.bytes[payload.offset + j] == NRTSuggesterBuilder.PAYLOAD_SEP) {
                        surfaceFormLen = j;
                        break;
                    }
                }
                assert surfaceFormLen != -1 : "no payloadSep found, unable to determine surface form";
                spare.copyUTF8Bytes(payload.bytes, payload.offset, surfaceFormLen);
                ByteArrayDataInput inputDocID = new ByteArrayDataInput(payload.bytes, surfaceFormLen + payload.offset + 1, payload.length - (surfaceFormLen + payload.offset));
                currentResults.add(new Entry<>(spare.toString(), weight, inputDocID.readVInt()));
            }
            results.put(currentKeyBuilder.toBytesRef(), new ArrayList<>(currentResults));
            int acceptedResultCount = currentResults.size();
            currentResults.clear();
            return acceptedResultCount;
        }
    }
}
