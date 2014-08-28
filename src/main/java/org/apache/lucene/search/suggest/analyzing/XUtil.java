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

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.*;

public class XUtil {
    /** Utility class to find top N shortest paths from start
     *  point(s). */
    public static class TopNSearcher<T> {

        private final FST<T> fst;
        private final FST.BytesReader bytesReader;
        private final int topN;
        private final int maxQueueDepth;

        private final FST.Arc<T> scratchArc = new FST.Arc<>();

        final Comparator<T> comparator;

        TreeSet<Util.FSTPath<T>> queue = null;

        /**
         * Creates an unbounded TopNSearcher
         * @param fst the {@link org.apache.lucene.util.fst.FST} to search on
         * @param topN the number of top scoring entries to retrieve
         * @param maxQueueDepth the maximum size of the queue of possible top entries
         * @param comparator the comparator to select the top N
         */
        public TopNSearcher(FST<T> fst, int topN, int maxQueueDepth, Comparator<T> comparator) {
            this.fst = fst;
            this.bytesReader = fst.getBytesReader();
            this.topN = topN;
            this.maxQueueDepth = maxQueueDepth;
            this.comparator = comparator;

            queue = new TreeSet<>(new TieBreakByInputComparator<>(comparator));
        }

        // If back plus this arc is competitive then add to queue:
        protected void addIfCompetitive(Util.FSTPath<T> path) {

            assert queue != null;

            T cost = fst.outputs.add(path.cost, path.arc.output);
            System.out.println("  addIfCompetitive queue.size()=" + queue.size() + " path=" + path + " + label=" + path.arc.label);

            if (queue.size() == maxQueueDepth) {
                Util.FSTPath<T> bottom = queue.last();
                int comp = comparator.compare(cost, bottom.cost);
                if (comp > 0) {
                    // Doesn't compete
                    return;
                } else if (comp == 0) {
                    // Tie break by alpha sort on the input:
                    path.input.grow(path.input.length+1);
                    path.input.ints[path.input.length++] = path.arc.label;
                    final int cmp = bottom.input.compareTo(path.input);
                    path.input.length--;

                    // We should never see dups:
                    assert cmp != 0;

                    if (cmp < 0) {
                        // Doesn't compete
                        return;
                    }
                }
                // Competes
            } else {
                // Queue isn't full yet, so any path we hit competes:
            }

            // copy over the current input to the new input
            // and add the arc.label to the end
            IntsRef newInput = new IntsRef(path.input.length+1);
            System.arraycopy(path.input.ints, 0, newInput.ints, 0, path.input.length);
            newInput.ints[path.input.length] = path.arc.label;
            newInput.length = path.input.length+1;
            final Util.FSTPath<T> newPath = new Util.FSTPath<>(cost, path.arc, newInput);

            queue.add(newPath);

            if (queue.size() == maxQueueDepth+1) {
                queue.pollLast();
            }
        }

        /** Adds all leaving arcs, including 'finished' arc, if
         *  the node is final, from this node into the queue.  */
        public void addStartPaths(FST.Arc<T> node, T startOutput, boolean allowEmptyString, IntsRef input) throws IOException {

            // De-dup NO_OUTPUT since it must be a singleton:
            if (startOutput.equals(fst.outputs.getNoOutput())) {
                startOutput = fst.outputs.getNoOutput();
            }

            Util.FSTPath<T> path = new Util.FSTPath<>(startOutput, node, input);
            fst.readFirstTargetArc(node, path.arc, bytesReader);

            System.out.println("add start paths");

            // Bootstrap: find the min starting arc
            while (true) {
                if (allowEmptyString || path.arc.label != FST.END_LABEL) {
                    addIfCompetitive(path);
                }
                if (path.arc.isLast()) {
                    break;
                }
                fst.readNextArc(path.arc, bytesReader);
            }
        }

        public TopResults<T> search() throws IOException {

            final List<Util.Result<T>> results = new ArrayList<>();

            System.out.println("search topN=" + topN);

            final FST.BytesReader fstReader = fst.getBytesReader();
            final T NO_OUTPUT = fst.outputs.getNoOutput();

            // TODO: we could enable FST to sorting arcs by weight
            // as it freezes... can easily do this on first pass
            // (w/o requiring rewrite)

            // TODO: maybe we should make an FST.INPUT_TYPE.BYTE0.5!?
            // (nibbles)
            int rejectCount = 0;

            // For each top N path:
            while (results.size() < topN) {
                System.out.println("\nfind next path: queue.size=" + queue.size());

                Util.FSTPath<T> path;

                if (queue == null) {
                    // Ran out of paths
                    System.out.println("  break queue=null");
                    break;
                }

                // Remove top path since we are now going to
                // pursue it:
                path = queue.pollFirst();

                if (path == null) {
                    // There were less than topN paths available:
                    System.out.println("  break no more paths");
                    break;
                }

                if (path.arc.label == FST.END_LABEL) {
                    System.out.println("    empty string!  cost=" + path.cost);
                    // Empty string!
                    path.input.length--;
                    results.add(new Util.Result<>(path.input, path.cost));
                    continue;
                }

                if (results.size() == topN-1 && maxQueueDepth == topN) {
                    // Last path -- don't bother w/ queue anymore:
                    queue = null;
                }

                System.out.println("  path: " + path);

                // We take path and find its "0 output completion",
                // ie, just keep traversing the first arc with
                // NO_OUTPUT that we can find, since this must lead
                // to the minimum path that completes from
                // path.arc.

                // For each input letter:
                while (true) {

                    System.out.println("\n    cycle path: " + path);
                    fst.readFirstTargetArc(path.arc, path.arc, fstReader);

                    // For each arc leaving this node:
                    boolean foundZero = false;
                    while(true) {
                        System.out.println("      arc=" + (char) path.arc.label + " cost=" + path.arc.output);
                        // tricky: instead of comparing output == 0, we must
                        // express it via the comparator compare(output, 0) == 0
                        if (comparator.compare(NO_OUTPUT, path.arc.output) == 0) {
                            if (queue == null) {
                                foundZero = true;
                                break;
                            } else if (!foundZero) {
                                scratchArc.copyFrom(path.arc);
                                foundZero = true;
                            } else {
                                addIfCompetitive(path);
                            }
                        } else if (queue != null) {
                            addIfCompetitive(path);
                        }
                        if (path.arc.isLast()) {
                            break;
                        }
                        fst.readNextArc(path.arc, fstReader);
                    }

                    assert foundZero;

                    if (queue != null) {
                        // TODO: maybe we can save this copyFrom if we
                        // are more clever above... eg on finding the
                        // first NO_OUTPUT arc we'd switch to using
                        // scratchArc
                        path.arc.copyFrom(scratchArc);
                    }

                    if (path.arc.label == FST.END_LABEL) {
                        // Add final output:
                        System.out.println("    done!: " + path);
                        T finalOutput = fst.outputs.add(path.cost, path.arc.output);
                        if (acceptResult(path.input, finalOutput)) {
                            System.out.println("    add result: " + path);
                            results.add(new Util.Result<>(path.input, finalOutput));
                        } else {
                            rejectCount++;
                        }
                        break;
                    } else {
                        path.input.grow(1+path.input.length);
                        path.input.ints[path.input.length] = path.arc.label;
                        path.input.length++;
                        path.cost = fst.outputs.add(path.cost, path.arc.output);
                    }
                }
            }
            return new TopResults<>(rejectCount + topN <= maxQueueDepth, results);
        }

        protected boolean acceptResult(IntsRef input, T output) {
            return true;
        }
    }
    /** Compares first by the provided comparator, and then
     *  tie breaks by path.input. */
    private static class TieBreakByInputComparator<T> implements Comparator<Util.FSTPath<T>> {
        private final Comparator<T> comparator;
        public TieBreakByInputComparator(Comparator<T> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Util.FSTPath<T> a, Util.FSTPath<T> b) {
            int cmp = comparator.compare(a.cost, b.cost);
            if (cmp == 0) {
                return a.input.compareTo(b.input);
            } else {
                return cmp;
            }
        }
    }

    /**
     * Holds the results for a top N search using {@link TopNSearcher}
     */
    public static final class TopResults<T> implements Iterable<Util.Result<T>> {

        /**
         * <code>true</code> iff this is a complete result ie. if
         * the specified queue size was large enough to find the complete list of results. This might
         * be <code>false</code> if the {@link TopNSearcher} rejected too many results.
         */
        public final boolean isComplete;
        /**
         * The top results
         */
        public final List<Util.Result<T>> topN;

        TopResults(boolean isComplete, List<Util.Result<T>> topN) {
            this.topN = topN;
            this.isComplete = isComplete;
        }

        @Override
        public Iterator<Util.Result<T>> iterator() {
            return topN.iterator();
        }
    }
}
