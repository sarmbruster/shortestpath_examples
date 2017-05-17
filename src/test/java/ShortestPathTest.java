import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ShortestPathTest {

    private GraphDatabaseService db;
    private static final String[] REL_TYPES = {"STATE", "PARENT", "FLAG"};
    private Node start;
    private Node end;
    private Collection<Node> nodesTouchedByFilter;

    @Before
    public void setup() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // populate graph
        final Map<String, Object> startEnd = Iterators.single(db.execute("create (s {name:'Intl\\'FF Autos'})<-[:STATE]-({name:'FL-3'})<-[:FLAG]-({name:'Toys&Hobbies'})<-[:STATE]-({name:'220'})<-[:PARENT]-(e {name:'Models&Kits'}) return s,e"));
        start = (Node) startEnd.get("s");
        end = (Node) startEnd.get("e");
        nodesTouchedByFilter = new HashSet<>();
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void shortestPathWithPathExpanderBuilder() {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (String reltype : REL_TYPES) {
            builder = builder.add(RelationshipType.withName(reltype), Direction.BOTH);
        }
        builder = builder.addNodeFilter(node -> {
            System.out.println("checking for node " + node.getProperty("name"));
            nodesTouchedByFilter.add(node);
            return true;
        });

        runShortestPathWithExpander(builder.build());
    }

    @Test
    public void shortestPathWithCustomPathExpander() {
        runShortestPathWithExpander(new FilteringPathExpander(node -> {
            nodesTouchedByFilter.add(node);
            return true;
        }, "STATE", "PARENT", "FLAG"));
    }

    private void runShortestPathWithExpander(PathExpander pathExpander) {
        try (Transaction tx = db.beginTx()) {
            PathExpander expander = new DebuggingPathExpander(pathExpander);
            final PathFinder<Path> pathFinder = GraphAlgoFactory.shortestPath(expander, 1000);
            final Path path = pathFinder.findSinglePath(start, end);
            assertNotNull("no shortest path found", path);
            assertEquals("wrong length of shortest path", 4, path.length());
            assertEquals("not all nodes were touch by filter", Iterables.count(db.getAllNodes()), nodesTouchedByFilter.size());
            tx.success();
        }
    }

    public static class DebuggingPathExpander implements PathExpander {

        private final PathExpander delegate;

        public DebuggingPathExpander(PathExpander delegate) {
            this.delegate = delegate;
        }

        @Override
        public Iterable<Relationship> expand(Path path, BranchState state) {

            System.out.println("current path: from : " + path.startNode().getProperty("name") + " to: " +
            path.endNode().getProperty("name") + ", length: " + path.length());
            Iterable result = delegate.expand(path, state);
            System.out.println("expanding " + Iterables.asList(result));
            return result;
        }

        @Override
        public PathExpander reverse() {
            return new DebuggingPathExpander(delegate.reverse());
        }
    }

}
