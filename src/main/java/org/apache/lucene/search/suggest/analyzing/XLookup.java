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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Simple Lookup interface for {@link CharSequence} suggestions.
 * @lucene.experimental
 */
public abstract class XLookup extends Lookup {

    /**
     * Result of a lookup.
     * @lucene.experimental
     */
    public static final class XLookupResult implements Comparable<XLookupResult> {
        /** the key's text */
        public final CharSequence key;

        /** Expert: custom Object to hold the result of a
         *  highlighted suggestion. */
        public final Object highlightKey;

        /** the key's weight */
        public final long value;

        /** the key's payload (null if not present) */
        public final BytesRef payload;

        /** list of returned stored field values */
        public final List<XStoredField> storedFields;

        /**
         * Encapsulates value(s) for a single stored field
         */
        public static final class XStoredField {

            Object[] values;
            final int numValues;
            int index = -1;
            public final String name;

            XStoredField(String name, IndexableField[] fields) {
                this.name = name;
                this.numValues = fields.length;
                this.values = new Object[numValues];

                for (IndexableField field : fields) {
                    boolean valueAdded = add(field.numericValue()) || add(field.binaryValue()) || add(field.stringValue());
                    if (!valueAdded) {
                        throw new UnsupportedOperationException("Field: " + name + " has to be of string or binary or number type");
                    }
                }
            }

            private boolean add(Object value) {
                if (value == null) {
                    return false;
                }
                if (++index < numValues) {
                    this.values[index] = value;
                } else {
                    assert false : "Object array size=" + numValues + " attempting to add " + index + " items";
                }
                return true;
            }

            /**
             * Returns a List of {@link java.lang.Number} for a stored field
             * or <code>null</code> if the stored field has no numeric values
             */
            public List<Number> getNumericValues() {
                List<Number> numericValues = null;
                for (Object value : values) {
                    if (value instanceof Number) {
                        if (numericValues == null) {
                            numericValues = new ArrayList<>(numValues);
                        }
                        numericValues.add((Number) value);
                    }
                }
                return numericValues;
            }

            /**
             * Returns a List of {@link java.lang.String} for a stored field
             * or <code>null</code> if the stored field has no string values
             */
            public List<String> getStringValues() {
                List<String> stringValues = null;
                for (Object value : values) {
                    if (value instanceof String) {
                        if (stringValues == null) {
                            stringValues = new ArrayList<>(numValues);
                        }
                        stringValues.add((String) value);
                    }
                }
                return stringValues;
            }

            /**
             * Returns a List of {@link org.apache.lucene.util.BytesRef} for a stored field
             * or <code>null</code> if the stored field has no binary values
             */
            public List<BytesRef> getBinaryValues() {
                List<BytesRef> binaryValues = null;
                for (Object value : values) {
                    if (value instanceof BytesRef) {
                        if (binaryValues == null) {
                            binaryValues = new ArrayList<>(numValues);
                        }
                        binaryValues.add((BytesRef) value);
                    }
                }
                return binaryValues;
            }


            /**
             * Returns a List of {@link java.lang.Object} for a stored field
             */
            public Object[] getValues() {
                assert index == numValues - 1;
                return values;
            }

            @Override
            public String toString() {
                StringBuilder stringBuilder = new StringBuilder("<" + name + ":[");
                for (int i=0; i < values.length; i++) {
                    stringBuilder.append(values[i].toString());
                    if (i != values.length-1) {
                        stringBuilder.append(", ");
                    }
                }
                stringBuilder.append("]");
                return stringBuilder.toString();
            }
        }

        static List<XStoredField> getStoredFieldsFromDocument(final Document document, final Set<String> storedFieldNames) {
            assert storedFieldNames != null;
            assert document != null;
            List<XStoredField> storedFields = new ArrayList<>(storedFieldNames.size());
            for (String payloadFieldName : storedFieldNames) {
                storedFields.add(new XStoredField(payloadFieldName, document.getFields(payloadFieldName)));
            }
            return storedFields;
        }

        /**
         * Create a new result from a key+weight+payload+storedFields.
         */
        public XLookupResult(CharSequence key, long value, BytesRef payload, List<XStoredField> storedFields) {
            this(key, null, value, payload, storedFields);
        }

        /**
         * Create a new result from a key+highlightKey+weight+payload+storedFields.
         */
        public XLookupResult(CharSequence key, Object highlightKey, long value, BytesRef payload, List<XStoredField> storedFields) {
            this.key = key;
            this.highlightKey = highlightKey;
            this.value = value;
            this.payload = payload;
            this.storedFields = storedFields;
        }

        @Override
        public String toString() {
            return key + "/" + value;
        }

        /** Compare alphabetically. */
        @Override
        public int compareTo(XLookupResult o) {
            return CHARSEQUENCE_COMPARATOR.compare(key, o.key);
        }
    }

    /**
     * Encapsulates all options that are accepted by the lookup method
     */
    public final static class XLookupOptions {
        final CharSequence key;
        final int num;
        final AtomicReader reader;
        final Set<String> payloadFields;
        boolean duplicateSurfaceForm = false;

        public XLookupOptions(CharSequence key, int num, AtomicReader reader) {
            this(key, num, reader, null, false);
        }

        public XLookupOptions(CharSequence key, int num, AtomicReader reader, boolean duplicateSurfaceForm) {
            this(key, num, reader, null, duplicateSurfaceForm);
        }

        public XLookupOptions(CharSequence key, int num, AtomicReader reader, Set<String> payloadFields) {
            this(key, num, reader, payloadFields, false);
        }

        public XLookupOptions(CharSequence key, int num, AtomicReader reader, Set<String> payloadFields, boolean duplicateSurfaceForm) {
            this.key = key;
            this.num = num;
            this.reader = reader;
            this.payloadFields = payloadFields;
            this.duplicateSurfaceForm = duplicateSurfaceForm;
        }
    }

    /**
     * A simple char-by-char comparator for {@link CharSequence}
     */
    public static final Comparator<CharSequence> CHARSEQUENCE_COMPARATOR = new CharSequenceComparator();

    private static class CharSequenceComparator implements Comparator<CharSequence> {

        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            final int l1 = o1.length();
            final int l2 = o2.length();

            final int aStop = Math.min(l1, l2);
            for (int i = 0; i < aStop; i++) {
                int diff = o1.charAt(i) - o2.charAt(i);
                if (diff != 0) {
                    return diff;
                }
            }
            // One is a prefix of the other, or, they are equal:
            return l1 - l2;
        }

    }

    /**
     * A {@link org.apache.lucene.util.PriorityQueue} collecting a fixed size of high priority {@link org.apache.lucene.search.suggest.analyzing.XLookup.XLookupResult}
     */
    public static final class XLookupPriorityQueue extends PriorityQueue<XLookupResult> {
        // TODO: should we move this out of the interface into a utility class?
        /**
         * Creates a new priority queue of the specified size.
         */
        public XLookupPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(XLookupResult a, XLookupResult b) {
            return a.value < b.value;
        }

        /**
         * Returns the top N results in descending order.
         * @return the top N results in descending order.
         */
        public XLookupResult[] getResults() {
            int size = size();
            XLookupResult[] res = new XLookupResult[size];
            for (int i = size - 1; i >= 0; i--) {
                res[i] = pop();
            }
            return res;
        }
    }

    /**
     * Sole constructor. (For invocation by subclass
     * constructors, typically implicit.)
     */
    public XLookup() {}


    /**
     * Look up a key and return possible completion for this key.
     * @param key lookup key. Depending on the implementation this may be
     * a prefix, misspelling, or even infix.
     * @param contexts contexts to filter the lookup by, or null if all contexts are allowed; if the suggestion contains any of the contexts, it's a match
     * @param onlyMorePopular return only more popular results
     * @param num maximum number of results to return
     * @return a list of possible completions, with their relative weight (e.g. popularity)
     */
    @Override
    public  List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) throws IOException {
        throw new UnsupportedOperationException();
    }

    public List<XLookupResult> lookup(CharSequence key, int num) {
        return lookup(new XLookupOptions(key, num, null));
    }

    public List<XLookupResult> lookup(CharSequence key, int num, AtomicReader reader) {
        return lookup(new XLookupOptions(key, num, reader));
    }

    public List<XLookupResult> lookup(final CharSequence key, int num, final AtomicReader reader, Set<String> payloadFields) {
        return lookup(new XLookupOptions(key, num, reader, payloadFields));
    }

    public abstract List<XLookupResult> lookup(final XLookupOptions lookupOptions);

    /**
     * Persist the constructed lookup data to a directory. Optional operation.
     * @param output {@link DataOutput} to write the data to.
     * @return true if successful, false if unsuccessful or not supported.
     * @throws IOException when fatal IO error occurs.
     */
    @Override
    public boolean store(DataOutput output) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Discard current lookup data and load it from a previously saved copy.
     * Optional operation.
     * @param input the {@link DataInput} to load the lookup data.
     * @return true if completed successfully, false if unsuccessful or not supported.
     * @throws IOException when fatal IO error occurs.
     */
    @Override
    public boolean load(DataInput input) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the number of entries the lookup was built with
     * @return total number of suggester entries
     */
    @Override
    public long getCount() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Builds up a new internal {@link XLookup} representation based on the given {@link org.apache.lucene.search.suggest.InputIterator}.
     * The implementation might re-sort the data internally.
     */
    @Override
    public void build(InputIterator inputIterator) throws IOException {
        throw new UnsupportedOperationException();
    }
}

