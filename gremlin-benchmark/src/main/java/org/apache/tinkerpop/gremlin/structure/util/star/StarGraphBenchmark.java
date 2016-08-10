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
package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.List;

@State(Scope.Thread)
public class StarGraphBenchmark extends AbstractBenchmarkBase {

    private Graph graph;
    private GraphTraversalSource g;
    private Vertex vertex;
    private StarGraph starGraph;

    @Setup
    public void prepare() throws IOException {
        graph = TinkerGraph.open();
        g = graph.traversal();
        vertex = g.addV("test").
                        property("name", "Emily").
                        property("age", 23).
                        property("height", 5.5).
                        property("eyes", "blue").next();
        // add properties
        vertex.property("a", "b");
        vertex.property("b", "c");
        vertex.property("d", "e");
        vertex.property("f", "b");
        vertex.property("a", "b");
        for(int i = 0; i < 100; i++) {
            final Vertex another = g.addV("test").next();
            addProperties(vertex.addEdge("knows", another), 5);
        }
        for(int i = 0; i < 100; i++) {
            Vertex another = g.addV("test").next();
            addProperties(another.addEdge("knows", vertex), 5);
        }

        starGraph = StarGraph.of(vertex);
    }

    private void addProperties(final Element element, final int propertyCount) {
        for(int i = 0; i < propertyCount; i++) {
            final String val = String.valueOf(i);
            element.property(val, val);
        }
    }

    @Benchmark
    public StarGraph testCreateStarGraph() {
        return StarGraph.of(vertex);
    }

    @Benchmark
    public List<Edge> getOutStarEdges() {
        List<Edge> edges = IteratorUtils.asList(starGraph.getStarVertex().edges(Direction.OUT, "knows"));
        assert edges.size() == 100;
        return edges;
    }

    @Benchmark
    public List<Edge> getInStarEdges() {
        List<Edge> edges = IteratorUtils.asList(starGraph.getStarVertex().edges(Direction.IN, "knows"));
        assert edges.size() == 100;
        return edges;
    }

    @Benchmark
    public List<VertexProperty> getVertexAllProperties() {
        List<VertexProperty> properties = IteratorUtils.asList(starGraph.getStarVertex().properties());
        assert properties.size() == 4;
        return properties;
    }

    @Benchmark
    public List<VertexProperty> getVertexSpecifiedProperties() {
        List<VertexProperty> properties = IteratorUtils.asList(starGraph.getStarVertex().properties("name", "age", "height"));
        assert properties.size() == 3;
        return properties;
    }

    @Benchmark
    public List<Property> getEdgeAllProperties() {
        return IteratorUtils.asList(starGraph.getStarVertex().edges(Direction.OUT, "knows").next().properties());
    }

    @Benchmark
    public List<Property> getEdgeSpecifiedProperties() {
        return IteratorUtils.asList(starGraph.getStarVertex().edges(Direction.OUT, "knows").next().properties("1", "2", "3"));
    }
}