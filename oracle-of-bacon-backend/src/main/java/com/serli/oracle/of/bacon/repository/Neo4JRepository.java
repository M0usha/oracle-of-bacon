package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Path;

import java.util.LinkedList;
import java.util.List;

public class Neo4JRepository {

    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "#neo4j1*"));
    }

    public List<GraphItem> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        Transaction t = session.beginTransaction();
        String query = "match (kb:Actors {name: 'Bacon, Kevin (I)'}), (a:Actors {name: {targetActorName}}), p = shortestPath((kb)-[:PLAYED_IN*]-(a)) return p";
        StatementResult res = t.run(query, Values.parameters("targetActorName", actorName));

        List<GraphItem> graph = new LinkedList<>();

        while (res.hasNext()) {
            res.next().values().forEach(val -> {
                Path p = val.asPath();

                p.nodes().forEach(node -> {
                        String nodeType = node.labels().iterator().next();
                        graph.add(new GraphNode(node.id(),
                                node.get("Actors".equals(nodeType) ? "name" : "title").asString(),
                                nodeType));
                    }
                );

                p.relationships().forEach(rel -> graph.add(new GraphEdge(
                        rel.id(),
                        rel.startNodeId(),
                        rel.endNodeId(),
                        rel.type()
                )));
            });
        }

        return graph;
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ \"data\": {");
                builder.append("\"id\": "+id+",");
                builder.append("\"type\": \""+type+"\",");
                builder.append("\"value\": \""+value+"\"");
            builder.append("}}");
            return builder.toString();
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ \"data\": {");
                builder.append("\"id\": "+id+",");
                builder.append("\"source\": "+source+",");
                builder.append("\"target\": "+target+",");
                builder.append("\"value\": \""+value+"\"");
            builder.append("}}");
            return builder.toString();
        }
    }
}
