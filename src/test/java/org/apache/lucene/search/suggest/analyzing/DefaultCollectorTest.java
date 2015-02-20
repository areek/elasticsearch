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

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

import java.util.*;

public class DefaultCollectorTest extends LuceneTestCase {


    @Test
    public void testMultiValuedFields() throws Exception {}

    @Test
    public void testMissingValues() throws Exception {}

    @Test
    public void testFieldValueRetrieval() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        int nDocs = atLeast(100);

        Document doc = new Document();
        Field idField = newStringField("id", "", Field.Store.YES);
        Field stringField = newStringField("string_field", "", Field.Store.YES);
        Field binaryDocValuesField = new BinaryDocValuesField("binary_doc_values", new BytesRef());
        Field numericDocValuesField = new NumericDocValuesField("numeric_doc_values", 0l);
        Field sortedDocValuesField = new SortedDocValuesField("sorted_doc_values", new BytesRef());
        Field sortedNumericValuesField = new SortedNumericDocValuesField("sorted_num_doc_values", 0l);
        Field sortedSetDocValuesField = new SortedSetDocValuesField("sorted_set_doc_values", new BytesRef());

        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("id");
        fieldNames.add("string_field");
        fieldNames.add("binary_doc_values");
        fieldNames.add("numeric_doc_values");
        fieldNames.add("sorted_doc_values");
        fieldNames.add("sorted_num_doc_values");
        fieldNames.add("sorted_set_doc_values");

        doc.add(idField);
        doc.add(stringField);
        doc.add(binaryDocValuesField);
        doc.add(numericDocValuesField);
        doc.add(sortedDocValuesField);
        doc.add(sortedNumericValuesField);
        doc.add(sortedSetDocValuesField);


        int[] docIds = new int[nDocs];
        for (int i = 0; i < nDocs; i++) {
            idField.setStringValue(String.valueOf(i));
            stringField.setStringValue(TestUtil.randomUnicodeString(random(), 10));
            binaryDocValuesField.setBytesValue(new BytesRef(TestUtil.randomUnicodeString(random(), 10)));
            numericDocValuesField.setLongValue(TestUtil.nextLong(random(),1, 1000));
            sortedDocValuesField.setBytesValue(new BytesRef(TestUtil.randomUnicodeString(random(), 10)));
            sortedNumericValuesField.setLongValue(TestUtil.nextLong(random(),1, 1000));
            sortedSetDocValuesField.setBytesValue(new BytesRef(TestUtil.randomUnicodeString(random(), 10)));
            iw.addDocument(doc);
            docIds[i] = i;

            if (random().nextInt(5) == 1) {
                iw.commit();
            }
        }

        iw.forceMerge(1);

        SegmentReader reader = getOnlySegmentReader(iw.getReader());

        DefaultCollector<Long> defaultCollector = new DefaultCollector<>(reader, fieldNames);

        defaultCollector.collect("", collect(docIds));

        List<DefaultCollector.Result<Long>> results = defaultCollector.get();

        for (DefaultCollector.Result.InnerResult<Long> longInnerResult : results.get(0).innerResults()) {
            for (DefaultCollector.FieldValue fieldValue : longInnerResult.fieldValues) {
                //TODO
            }
        }

        iw.close();
        reader.close();
        dir.close();
    }

    private static List<SegmentLookup.Collector.ResultMetaData<Long>> collect(int... docIDs) {
        List<SegmentLookup.Collector.ResultMetaData<Long>> res = new ArrayList<>();
        for (int docID : docIDs) {
            res.add(new SegmentLookup.Collector.ResultMetaData<>("", 1l, docID));
        }
        return res;
    }
}
