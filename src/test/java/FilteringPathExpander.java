import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.Direction.*;

class FilteringPathExpander implements PathExpander {

    private final Predicate<Node> nodeFilter;
    private Collection<Pair<RelationshipType, Direction>> relTypeAndDirections = new ArrayList<>();
    private boolean reversed = false;

    public FilteringPathExpander(Predicate<Node> nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    public FilteringPathExpander addTypeAndDirection(RelationshipType type, Direction dir) {
        relTypeAndDirections.add(Pair.of(type, dir));
        return this;
    }

    private FilteringPathExpander(Predicate<Node> nodeFilter, boolean reversed, Collection<Pair<RelationshipType, Direction>> relTypeAndDirections) {
        this.nodeFilter = nodeFilter;
        this.reversed = reversed;
        this.relTypeAndDirections = relTypeAndDirections;
    }

    public FilteringPathExpander addTypeAndDirections(Object... typeOrDir) {
        if ((typeOrDir.length % 2) != 0) {
            throw new IllegalArgumentException("supply even number of parameters");
        }

        for (int i=0; i<typeOrDir.length; i=i+2) {
            Direction dir = (Direction) typeOrDir[i+1];
            RelationshipType reltype = null;
            Object type = typeOrDir[i];
            if (type instanceof RelationshipType) {
                reltype = (RelationshipType) type;
            } else if (type instanceof String) {
                reltype = RelationshipType.withName((String) type);
            } else {
                throw new IllegalArgumentException("invalid type: " + type.getClass());
            }
            addTypeAndDirection(reltype, dir);
        }
        return this;
    }


    @Override
    public Iterable<Relationship> expand(Path path, BranchState state) {
        return Iterables.concat(
                relTypeAndDirections.stream()
                .map(pair -> filteredRelationshipsByType(path.endNode(), pair.first(), pair.other()))
                .collect(Collectors.toList())
        );
    }

    private Iterable<Relationship> filteredRelationshipsByType(Node currentNode, RelationshipType type, Direction direction) {
        final Iterable<Relationship> relationships = currentNode.getRelationships(type, flipDirectionIfReversed(direction));
        return StreamSupport.stream(relationships.spliterator(), false)
                .filter(relationship -> filterExpandedNode(relationship, currentNode))
                .collect(Collectors.toList());
    }

    private Direction flipDirectionIfReversed(Direction direction) {
        switch (direction) {
            case BOTH:
                return BOTH;
            case INCOMING:
                return reversed ? OUTGOING : INCOMING;
            case OUTGOING:
                return reversed ? INCOMING : OUTGOING;
            default:
                throw new IllegalArgumentException();
        }
    }

    private boolean filterExpandedNode(Relationship relationship, Node endNode) {
        Node node = relationship.getOtherNode(endNode);
        System.out.println("checking for node " + node.getProperty("name"));
        return nodeFilter.test(node);
    }

    @Override
    public PathExpander reverse() {
        return new FilteringPathExpander(nodeFilter, !reversed, relTypeAndDirections);
    }
}
