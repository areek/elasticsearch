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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.xdocument.CompletionQuery;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocs;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.*;

public class CompletionSuggester extends Suggester<CompletionSuggestionContext> {

    @Override
    public SuggestContextParser getContextParser() {
        return new CompletionSuggestParser(this);
    }

    @Override
    protected Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> innerExecute(String name,
            CompletionSuggestionContext suggestionContext, IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        if (suggestionContext.fieldType() == null) {
            throw new ElasticsearchException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, suggestionContext.getSize());
        spare.copyUTF8Bytes(suggestionContext.getText());
        TopSuggestDocsCollector collector = new TopSuggestDocsCollector(suggestionContext.getSize());
        suggest(searcher, toQuery(suggestionContext), collector);
        final TopSuggestDocs topSuggestDocs = collector.get();
        final ContextMappings contextMappings = suggestionContext.fieldType().getContextMappings();
        LinkedHashMap<Integer, CompletionSuggestion.Entry.Option> results = new LinkedHashMap<>(suggestionContext.getSize());
        for (TopSuggestDocs.SuggestScoreDoc suggestDoc : topSuggestDocs.scoreLookupDocs()) {
            // TODO: currently we can get multiple entries with the same docID
            // this has to be fixed at the lucene level
            // This has other implications:
            // if we index a suggestion with n contexts, the suggestion and all its contexts
            // would count as n hits rather than 1, so we have to multiply the desired size
            // with n to get a suggestion with all n contexts
            final Map.Entry<String, CharSequence> contextEntry;
            if (contextMappings != null && suggestDoc.context != null) {
                contextEntry = contextMappings.getNamedContext(suggestDoc.context);
            } else {
                assert suggestDoc.context == null;
                contextEntry = null;
            }
            final CompletionSuggestion.Entry.Option value = results.get(suggestDoc.doc);
            if (value == null) {
                final Map<String, List<Object>> payloads;
                if (!suggestionContext.fields().isEmpty()) {
                    int readerIndex = ReaderUtil.subIndex(suggestDoc.doc, searcher.getIndexReader().leaves());
                    LeafReaderContext subReaderContext = searcher.getIndexReader().leaves().get(readerIndex);
                    int subDocId = suggestDoc.doc - subReaderContext.docBase;
                    payloads = new HashMap<>(suggestionContext.fields().size());
                    for (String field : suggestionContext.fields()) {
                        MappedFieldType fieldType = suggestionContext.mapperService().smartNameFieldType(field);
                        if (fieldType != null) {
                            AtomicFieldData data = suggestionContext.fieldData().getForField(fieldType).load(subReaderContext);
                            ScriptDocValues scriptValues = data.getScriptValues();
                            scriptValues.setNextDocId(subDocId);
                            payloads.put(field, scriptValues.getValues());
                        }
                    }
                } else {
                    payloads = Collections.emptyMap();
                }
                final CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(new StringText(suggestDoc.key.toString()), suggestDoc.score, contextEntry, payloads);
                results.put(suggestDoc.doc, option);
            } else {
                value.addContextEntry(contextEntry);
                if (value.getScore() < suggestDoc.score) {
                    value.setScore(suggestDoc.score);
                }
            }
        }
        completionSuggestion.populateEntry(spare.toString(), results);
        return completionSuggestion;
    }

    /*
    TODO: should this be moved to CompletionSuggestionParser?
    so the CompletionSuggestionContext will have only the query instead
     */
    private CompletionQuery toQuery(CompletionSuggestionContext suggestionContext) throws IOException {
        CompletionFieldMapper.CompletionFieldType fieldType = suggestionContext.fieldType();
        final CompletionQuery query;
        if (suggestionContext.getPrefix() != null) {
            if (suggestionContext.getFuzzyOptionsBuilder() != null) {
                CompletionSuggestionBuilder.FuzzyOptionsBuilder fuzzyOptions = suggestionContext.getFuzzyOptionsBuilder();
                query = fieldType.fuzzyQuery(suggestionContext.getPrefix().utf8ToString(),
                        Fuzziness.fromEdits(fuzzyOptions.getEditDistance()),
                        fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(),
                        fuzzyOptions.getMaxDeterminizedStates(), fuzzyOptions.isTranspositions(),
                        fuzzyOptions.isUnicodeAware());
            } else {
                query = fieldType.prefixQuery(suggestionContext.getPrefix());
            }
        } else if (suggestionContext.getRegex() != null) {
            if (suggestionContext.getFuzzyOptionsBuilder() != null) {
                throw new IllegalArgumentException("can not use 'fuzzy' options with 'regex");
            }
            CompletionSuggestionBuilder.RegexOptionsBuilder regexOptions = suggestionContext.getRegexOptionsBuilder();
            if (regexOptions == null) {
                regexOptions = new CompletionSuggestionBuilder.RegexOptionsBuilder();
            }
            query = fieldType.regexpQuery(suggestionContext.getRegex(), regexOptions.getFlagsValue(),
                    regexOptions.getMaxDeterminizedStates());
        } else {
            throw new IllegalArgumentException("'prefix' or 'regex' must be defined");
        }
        if (fieldType.hasContextMappings()) {
            ContextMappings contextMappings = fieldType.getContextMappings();
            return contextMappings.toContextQuery(query, suggestionContext.getQueryContexts());
        }
        return query;
    }

    private static void suggest(IndexSearcher searcher, CompletionQuery query, TopSuggestDocsCollector collector) throws IOException {
        query = (CompletionQuery) query.rewrite(searcher.getIndexReader());
        Weight weight = query.createWeight(searcher, collector.needsScores());
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            BulkScorer scorer = weight.bulkScorer(context);
            if (scorer != null) {
                try {
                    scorer.score(collector.getLeafCollector(context), context.reader().getLiveDocs());
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                }
            }
        }
    }
}
