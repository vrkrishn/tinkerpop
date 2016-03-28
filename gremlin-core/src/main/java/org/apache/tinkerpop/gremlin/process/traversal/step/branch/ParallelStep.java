/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ParallelStep<S, E> extends AbstractStep<S, E> implements TraversalParent {

    //private final Queue<Traverser.Admin<E>> barrier = new ConcurrentLinkedQueue<>();
    private final TraverserSet<E> barrier = new TraverserSet<>();

    private boolean first = true;
    private final ExecutorService executor;
    private List<Callable<Boolean>> callables;
    private List<Future<Boolean>> futures;
    private List<Traversal.Admin<S, E>> threadedTraversals;

    private final Object barrierMutex = new Object();
    //private final Set<String> activeWorkers = new HashSet<>();

    public ParallelStep(final Traversal.Admin traversal, final int threads, final Traversal<S, E> threadedTraversal) {
        super(traversal);
        this.threadedTraversals = new ArrayList<>();
        this.executor = Executors.newFixedThreadPool(threads);
        this.callables = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final Traversal.Admin<S, E> parallelTraversal = threadedTraversal.asAdmin().clone();
            this.threadedTraversals.add(parallelTraversal);
            this.callables.add(() -> {
                try {
                    parallelTraversal.addStarts((Iterator) this.starts);
                    final Step<?, E> endStep = parallelTraversal.getEndStep();
                    while (true) {
                        //activeWorkers.add(Thread.currentThread().getName());
                        final Traverser.Admin<E> end = endStep.next();
                        synchronized (this.barrierMutex) {
                            this.barrier.add(end);
                        }
                    }
                } catch (final Exception e) {
                    // System.out.println(e);
                    // can get null pointer cause of threading that is pulling on previous step (need to create a thread safe barrier)
                }
                return true;
            });
        }
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return this.threadedTraversals;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.threadedTraversals.get(0).getTraverserRequirements();
    }

    public Traverser.Admin<E> processNextStart() {
        if (this.first) {
            this.first = false;
            try {
                this.futures = this.executor.invokeAll(this.callables);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        while (true) {
            if (!this.barrier.isEmpty())
                return this.barrier.remove();
            else if (this.futures.stream().map(Future::isDone).reduce((a, b) -> a && b).get()) {
                this.executor.shutdown();
                //System.out.println(this.activeWorkers);
                return this.barrier.remove();

            }
        }
    }
}
