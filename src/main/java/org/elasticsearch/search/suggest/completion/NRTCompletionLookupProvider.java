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

package org.elasticsearch.search.suggest.completion;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.XFuzzySuggester;
import org.apache.lucene.search.suggest.analyzing.XNRTSuggester;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.context.ContextMapping.ContextQuery;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.search.suggest.completion.AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder;


/**
 * TODO:
 *  - remove irrelevant codec versioning stuff
 *  - general refactoring
 */
public class NRTCompletionLookupProvider extends CompletionLookupProvider {

    // for serialization
    public static final int SERIALIZE_PRESERVE_SEPERATORS = 1;
    public static final int SERIALIZE_HAS_PAYLOADS = 2;
    public static final int SERIALIZE_PRESERVE_POSITION_INCREMENTS = 4;

    private static final int MAX_SURFACE_FORMS_PER_ANALYZED_FORM = 256;
    private static final int MAX_GRAPH_EXPANSIONS = -1;

    public static final String CODEC_NAME = "nrtsuggester";
    public static final int CODEC_VERSION_START = 1;
    public static final int CODEC_VERSION_SERIALIZED_LABELS = 2;
    public static final int CODEC_VERSION_CHECKSUMS = 3;
    public static final int CODEC_VERSION_LATEST = CODEC_VERSION_CHECKSUMS;

    private boolean preserveSep;
    private boolean preservePositionIncrements;
    private int maxSurfaceFormsPerAnalyzedForm;
    private int maxGraphExpansions;
    private boolean hasPayloads;
    private final XNRTSuggester prototype;

    public NRTCompletionLookupProvider(boolean preserveSep, boolean exactFirst, boolean preservePositionIncrements, boolean hasPayloads) {
        this.preserveSep = preserveSep;
        this.preservePositionIncrements = preservePositionIncrements;
        this.hasPayloads = hasPayloads;
        this.maxSurfaceFormsPerAnalyzedForm = MAX_SURFACE_FORMS_PER_ANALYZED_FORM;
        this.maxGraphExpansions = MAX_GRAPH_EXPANSIONS;
        int options = preserveSep ? XNRTSuggester.PRESERVE_SEP : 0;
        // needs to fixed in the suggester first before it can be supported
        //options |= exactFirst ? XNRTSuggester.EXACT_FIRST : 0;
        prototype = new XNRTSuggester(null, null, null, options, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions, preservePositionIncrements, null, false, 1, XNRTSuggester.SEP_LABEL, XNRTSuggester.PAYLOAD_SEP, XNRTSuggester.END_BYTE, XNRTSuggester.HOLE_CHARACTER);
    }

    @Override
    public String getName() {
        return "nrtsuggester";
    }

    @Override
    public FieldsConsumer consumer(final IndexOutput output) throws IOException {
        CodecUtil.writeHeader(output, CODEC_NAME, CODEC_VERSION_LATEST);
        return new FieldsConsumer() {
            private Map<FieldInfo, Long> fieldOffsets = new HashMap<>();

            @Override
            public void close() throws IOException {
                try {
                  /*
                   * write the offsets per field such that we know where
                   * we need to load the FSTs from
                   */
                    long pointer = output.getFilePointer();
                    output.writeVInt(fieldOffsets.size());
                    for (Map.Entry<FieldInfo, Long> entry : fieldOffsets.entrySet()) {
                        output.writeString(entry.getKey().name);
                        output.writeVLong(entry.getValue());
                    }
                    output.writeLong(pointer);
                    CodecUtil.writeFooter(output);
                } finally {
                    IOUtils.close(output);
                }
            }

            @Override
            public TermsConsumer addField(final FieldInfo field) throws IOException {

                return new TermsConsumer() {
                    final XNRTSuggester.XBuilder builder = new XNRTSuggester.XBuilder(maxSurfaceFormsPerAnalyzedForm, hasPayloads, XNRTSuggester.PAYLOAD_SEP);
                    final CompletionPostingsConsumer postingsConsumer = new CompletionPostingsConsumer(NRTCompletionLookupProvider.this, builder);

                    @Override
                    public PostingsConsumer startTerm(BytesRef text) throws IOException {
                        builder.startTerm(text);
                        return postingsConsumer;
                    }

                    @Override
                    public Comparator<BytesRef> getComparator() throws IOException {
                        return BytesRef.getUTF8SortedAsUnicodeComparator();
                    }

                    @Override
                    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
                        builder.finishTerm(stats.docFreq); // use  doc freq as a fallback
                    }

                    @Override
                    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
                        /*
                         * Here we are done processing the field and we can
                         * buid the FST and write it to disk.
                         */
                        FST<Pair<Long, BytesRef>> build = builder.build();
                        assert build != null || docCount == 0 : "the FST is null but docCount is != 0 actual value: [" + docCount + "]";
                        /*
                         * it's possible that the FST is null if we have 2 segments that get merged
                         * and all docs that have a value in this field are deleted. This will cause
                         * a consumer to be created but it doesn't consume any values causing the FSTBuilder
                         * to return null.
                         */
                        if (build != null) {
                            fieldOffsets.put(field, output.getFilePointer());
                            build.save(output);
                            /* write some more meta-info */
                            output.writeVInt(postingsConsumer.getMaxAnalyzedPathsForOneInput());
                            output.writeVInt(maxSurfaceFormsPerAnalyzedForm);
                            output.writeInt(maxGraphExpansions); // can be negative
                            int options = 0;
                            options |= preserveSep ? SERIALIZE_PRESERVE_SEPERATORS : 0;
                            options |= hasPayloads ? SERIALIZE_HAS_PAYLOADS : 0;
                            options |= preservePositionIncrements ? SERIALIZE_PRESERVE_POSITION_INCREMENTS : 0;
                            output.writeVInt(options);
                            output.writeVInt(XNRTSuggester.SEP_LABEL);
                            output.writeVInt(XNRTSuggester.END_BYTE);
                            output.writeVInt(XNRTSuggester.PAYLOAD_SEP);
                            output.writeVInt(XNRTSuggester.HOLE_CHARACTER);
                        }
                    }
                };
            }
        };
    }

    private static final class CompletionPostingsConsumer extends PostingsConsumer {
        private final SuggestPayload spare = new SuggestPayload();
        private NRTCompletionLookupProvider analyzingSuggestLookupProvider;
        private XNRTSuggester.XBuilder builder;
        private int maxAnalyzedPathsForOneInput = 0;

        public CompletionPostingsConsumer(NRTCompletionLookupProvider analyzingSuggestLookupProvider, XNRTSuggester.XBuilder builder) {
            this.analyzingSuggestLookupProvider = analyzingSuggestLookupProvider;
            this.builder = builder;
        }

        @Override
        public void startDoc(int docID, int freq) throws IOException {
            builder.setDocID(docID);
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
            analyzingSuggestLookupProvider.parsePayload(payload, spare);
            int count = builder.addSurface(spare.surfaceForm, spare.payload, spare.weight);
            // multi fields have the same surface form so we sum up here
            maxAnalyzedPathsForOneInput = Math.max(maxAnalyzedPathsForOneInput, count);
        }

        @Override
        public void finishDoc() throws IOException {
        }

        public int getMaxAnalyzedPathsForOneInput() {
            return maxAnalyzedPathsForOneInput;
        }
    }

    @Override
    public LookupFactory load(IndexInput input) throws IOException {
        long sizeInBytes = 0;
        int version = CodecUtil.checkHeader(input, CODEC_NAME, CODEC_VERSION_START, CODEC_VERSION_LATEST);
        if (version >= CODEC_VERSION_CHECKSUMS) {
            CodecUtil.checksumEntireFile(input);
        }
        final long metaPointerPosition = input.length() - (version >= CODEC_VERSION_CHECKSUMS? 8 + CodecUtil.footerLength() : 8);
        final Map<String, AnalyzingSuggestHolder> lookupMap = new HashMap<>();
        input.seek(metaPointerPosition);
        long metaPointer = input.readLong();
        input.seek(metaPointer);
        int numFields = input.readVInt();

        Map<Long, String> meta = new TreeMap<>();
        for (int i = 0; i < numFields; i++) {
            String name = input.readString();
            long offset = input.readVLong();
            meta.put(offset, name);
        }

        for (Map.Entry<Long, String> entry : meta.entrySet()) {
            input.seek(entry.getKey());
            final FST<Pair<Long, BytesRef>> fst = new FST<>(input, new PairOutputs<>(
                    PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()));
            int maxAnalyzedPathsForOneInput = input.readVInt();
            int maxSurfaceFormsPerAnalyzedForm = input.readVInt();
            int maxGraphExpansions = input.readInt();
            int options = input.readVInt();
            boolean preserveSep = (options & SERIALIZE_PRESERVE_SEPERATORS) != 0;
            boolean hasPayloads = (options & SERIALIZE_HAS_PAYLOADS) != 0;
            boolean preservePositionIncrements = (options & SERIALIZE_PRESERVE_POSITION_INCREMENTS) != 0;

            // first version did not include these three fields, so fall back to old default (before the analyzingsuggester
            // was updated in Lucene, so we cannot use the suggester defaults)
            int sepLabel, payloadSep, endByte, holeCharacter;
            switch (version) {
                case CODEC_VERSION_START:
                    sepLabel = 0xFF;
                    payloadSep = '\u001f';
                    endByte = 0x0;
                    holeCharacter = '\u001E';
                    break;
                default:
                    sepLabel = input.readVInt();
                    endByte = input.readVInt();
                    payloadSep = input.readVInt();
                    holeCharacter = input.readVInt();
            }

            AnalyzingSuggestHolder holder = new AnalyzingSuggestHolder(preserveSep, preservePositionIncrements, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions,
                    hasPayloads, maxAnalyzedPathsForOneInput, fst, sepLabel, payloadSep, endByte, holeCharacter);
            sizeInBytes += fst.ramBytesUsed();
            lookupMap.put(entry.getValue(), holder);
        }
        final long ramBytesUsed = sizeInBytes;
        return new LookupFactory() {
            @Override
            public Lookup getLookup(CompletionFieldMapper mapper, CompletionSuggestionContext suggestionContext) {
                AnalyzingSuggestHolder analyzingSuggestHolder = lookupMap.get(mapper.names().indexName());
                if (analyzingSuggestHolder == null) {
                    return null;
                }
                int flags = analyzingSuggestHolder.getPreserveSeparator() ? XNRTSuggester.PRESERVE_SEP : 0;

                final XNRTSuggester suggester;
                final Automaton queryPrefix = mapper.requiresContext() ? ContextQuery.toAutomaton(analyzingSuggestHolder.getPreserveSeparator(), suggestionContext.getContextQueries()) : null;

                /*
                if (suggestionContext.isFuzzy()) {
                    suggester = new XFuzzySuggester(mapper.indexAnalyzer(), queryPrefix, mapper.searchAnalyzer(), flags,
                            analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                            suggestionContext.getFuzzyEditDistance(), suggestionContext.isFuzzyTranspositions(),
                            suggestionContext.getFuzzyPrefixLength(), suggestionContext.getFuzzyMinLength(), suggestionContext.isFuzzyUnicodeAware(),
                            analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                            analyzingSuggestHolder.maxAnalyzedPathsForOneInput, analyzingSuggestHolder.sepLabel, analyzingSuggestHolder.payloadSep, analyzingSuggestHolder.endByte,
                            analyzingSuggestHolder.holeCharacter);
                } else {
                    */
                    suggester = new XNRTSuggester(mapper.indexAnalyzer(), queryPrefix, mapper.searchAnalyzer(), flags,
                            analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                            analyzingSuggestHolder.preservePositionIncrements, analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                            analyzingSuggestHolder.maxAnalyzedPathsForOneInput, analyzingSuggestHolder.sepLabel, analyzingSuggestHolder.payloadSep, analyzingSuggestHolder.endByte,
                            analyzingSuggestHolder.holeCharacter);
                //}
                return suggester;
            }

            @Override
            public CompletionStats stats(String... fields) {
                long sizeInBytes = 0;
                ObjectLongOpenHashMap<String> completionFields = null;
                if (fields != null  && fields.length > 0) {
                    completionFields = new ObjectLongOpenHashMap<>(fields.length);
                }

                for (Map.Entry<String, AnalyzingSuggestHolder> entry : lookupMap.entrySet()) {
                    sizeInBytes += entry.getValue().fst.ramBytesUsed();
                    if (fields == null || fields.length == 0) {
                        continue;
                    }
                    if (Regex.simpleMatch(fields, entry.getKey())) {
                        long fstSize = entry.getValue().fst.ramBytesUsed();
                        completionFields.addTo(entry.getKey(), fstSize);
                    }
                }

                return new CompletionStats(sizeInBytes, completionFields);
            }

            @Override
            AnalyzingSuggestHolder getAnalyzingSuggestHolder(CompletionFieldMapper mapper) {
                return lookupMap.get(mapper.names().indexName());
            }

            @Override
            public long ramBytesUsed() {
                return ramBytesUsed;
            }
        };
    }

    @Override
    public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
        return prototype.toFiniteStrings(prototype.getTokenStreamToAutomaton(), stream);
    }
}