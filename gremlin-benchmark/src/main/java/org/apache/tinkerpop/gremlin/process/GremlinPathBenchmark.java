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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.benchmark.util.AbstractGraphBenchmark;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/07/22
 * Time: 7:26 AM
 */
public class GremlinPathBenchmark extends AbstractGraphBenchmark {

    private Vertex a;
    private final static int COUNT = 1_000_000;

    @Setup
    public void prepare() throws IOException {
        super.prepare();
        a = graph.addVertex(T.label, "A", "name", "a1");
        for (int i = 1; i < COUNT; i++) {
            Vertex b = graph.addVertex(T.label, "B", "name", "name_" + i);
            a.addEdge("outB", b);
            for (int j = 0; j < 1; j++) {
                Vertex c = graph.addVertex(T.label, "C", "name", "name_" + i + " " + j);
                b.addEdge("outC", c);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void g_path() throws Exception {
        GraphTraversal<Vertex, Path> traversal = g.V(a).as("a").out().as("b").out().as("c").path();
        int count = 1;
        while (traversal.hasNext()) {
            Path path = traversal.next();
            count++;
        }
        assertEquals(COUNT, count);
    }

}
