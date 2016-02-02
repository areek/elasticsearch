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

import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class CompletionSuggestionContext extends SuggestionSearchContext.SuggestionContext {

    private CompletionFieldMapper.CompletionFieldType fieldType;
    private FuzzyOptions fuzzyOptions;
    private RegexOptions regexOptions;
    private Map<String, List<ContextMapping.QueryContext>> queryContexts = Collections.emptyMap();
    private final MapperService mapperService;
    private final IndexFieldDataService indexFieldDataService;
    private Set<String> payloadFields = Collections.emptySet();

    CompletionSuggestionContext(Suggester suggester, MapperService mapperService, IndexFieldDataService indexFieldDataService) {
        super(suggester);
        this.indexFieldDataService = indexFieldDataService;
        this.mapperService = mapperService;
    }

    CompletionFieldMapper.CompletionFieldType getFieldType() {
        return this.fieldType;
    }

    void setFieldType(CompletionFieldMapper.CompletionFieldType fieldType) {
        this.fieldType = fieldType;
    }

    void setRegexOptionsBuilder(RegexOptions.Builder regexOptionsBuilder) {
        this.regexOptions = regexOptionsBuilder.build();
    }

    void setFuzzyOptionsBuilder(FuzzyOptions.Builder fuzzyOptionsBuilder) {
        this.fuzzyOptions = fuzzyOptionsBuilder.build();
    }

    void setQueryContexts(Map<String, List<ContextMapping.QueryContext>> queryContexts) {
        this.queryContexts = queryContexts;
    }


    MapperService getMapperService() {
        return mapperService;
    }

    IndexFieldDataService getIndexFieldDataService() {
        return indexFieldDataService;
    }

    void setPayloadFields(Set<String> fields) {
        this.payloadFields = fields;
    }

    void setPayloadFields(List<String> fields) {
        setPayloadFields(new HashSet<>(fields));
    }

    Set<String> getPayloadFields() {
        return payloadFields;
    }

    CompletionQuery toQuery() {
        CompletionFieldMapper.CompletionFieldType fieldType = getFieldType();
        final CompletionQuery query;
        if (getPrefix() != null) {
            if (fuzzyOptions != null) {
                query = fieldType.fuzzyQuery(getPrefix().utf8ToString(),
                        Fuzziness.fromEdits(fuzzyOptions.getEditDistance()),
                        fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(),
                        fuzzyOptions.getMaxDeterminizedStates(), fuzzyOptions.isTranspositions(),
                        fuzzyOptions.isUnicodeAware());
            } else {
                query = fieldType.prefixQuery(getPrefix());
            }
        } else if (getRegex() != null) {
            if (fuzzyOptions != null) {
                throw new IllegalArgumentException("can not use 'fuzzy' options with 'regex");
            }
            if (regexOptions == null) {
                regexOptions = RegexOptions.builder().build();
            }
            query = fieldType.regexpQuery(getRegex(), regexOptions.getFlagsValue(),
                    regexOptions.getMaxDeterminizedStates());
        } else {
            throw new IllegalArgumentException("'prefix' or 'regex' must be defined");
        }
        if (fieldType.hasContextMappings()) {
            ContextMappings contextMappings = fieldType.getContextMappings();
            return contextMappings.toContextQuery(query, queryContexts);
        }
        return query;
    }
}
