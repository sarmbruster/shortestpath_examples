import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.Iterables;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class FilteringPathExpander implements PathExpander {

    private final String[] relTypes;
    private final Predicate<Node> nodeFilter;

    public FilteringPathExpander(Predicate<Node> nodeFilter, String... relTypes) {
        this.relTypes = relTypes;
        this.nodeFilter = nodeFilter;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState state) {
        return Iterables.concat(
                Stream.of(relTypes)
                .map(type -> filteredRelationshipsByType(path.endNode(), RelationshipType.withName(type)))
                .collect(Collectors.toList())
        );
    }

    private Iterable<Relationship> filteredRelationshipsByType(Node currentNode, RelationshipType type) {
        final Iterable<Relationship> relationships = currentNode.getRelationships(type);
        return StreamSupport.stream(relationships.spliterator(), false)
                .filter(relationship -> filterExpandedNode(relationship, currentNode))
                .collect(Collectors.toList());
    }

    private boolean filterExpandedNode(Relationship relationship, Node endNode) {
        Node node = relationship.getOtherNode(endNode);
        System.out.println("checking for node " + node.getProperty("name"));
        return nodeFilter.test(node);
    }

    @Override
    public PathExpander reverse() {
        return this;
    }
}
