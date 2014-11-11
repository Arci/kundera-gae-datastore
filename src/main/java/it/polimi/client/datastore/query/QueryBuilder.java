package it.polimi.client.datastore.query;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PropertyProjection;
import com.google.appengine.api.datastore.Query;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.query.KunderaQuery;
import it.polimi.client.datastore.DatastoreUtils;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import java.util.*;

/**
 * Helpful methods to translate from {@link com.impetus.kundera.query.KunderaQuery}
 * to {@link com.google.appengine.api.datastore.Query}.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.query.KunderaQuery
 * @see com.google.appengine.api.datastore.Query
 */
public class QueryBuilder {

    private Query query;
    private String kind;
    /**
     * true if {@link EntityType} of the query holds relationships with other entities
     */
    private boolean holdRelationships;
    /**
     * true if the select query is different from "SELECT *"
     */
    private boolean isProjectionQuery;
    private Map<String, Class> projections;
    private final EntityMetadata entityMetadata;
    private final EntityType entityType;

    public QueryBuilder(EntityMetadata entityMetadata, EntityType entityType, boolean holdRelationships) {
        this.entityMetadata = entityMetadata;
        this.entityType = entityType;
        this.kind = getEntityClass().getSimpleName();
        this.holdRelationships = holdRelationships;
        this.isProjectionQuery = false;
        this.projections = new HashMap<String, Class>();
    }

    public Query getQuery() {
        return this.query;
    }

    public Class getEntityClass() {
        return this.entityMetadata.getEntityClazz();
    }

    public boolean holdRelationships() {
        return this.holdRelationships;
    }

    public boolean isProjectionQuery() {
        return this.isProjectionQuery;
    }

    public Set<String> getProjections() {
        return this.projections.keySet();
    }

    public QueryBuilder setFrom(Class entityClass) {
        this.query = new Query(entityClass.getSimpleName());
        return this;
    }

    /**
     * Add multiple projections to the query.
     *
     * @param columns array of column names on which add a projection.
     *
     * @return this, for chaining.
     *
     * @throws com.impetus.kundera.KunderaException if Java type is not found for a column.
     * @see com.google.appengine.api.datastore.PropertyProjection
     */
    public QueryBuilder addProjections(String[] columns) {
        if (columns.length != 0) {
            this.isProjectionQuery = true;
            for (String column : columns) {
                try {
                    String filedName = entityMetadata.getFieldName(column);
                    Attribute attribute = entityType.getAttribute(filedName);
                    addProjection(column, attribute.getJavaType());
                } catch (NullPointerException npe) {
                    /* case attribute not found */
                    throw new KunderaException("Cannot find Java type for  [" + column + "]");
                }
            }
        }
        return this;
    }

    /**
     * Add a projection to the query.
     *
     * @param column     the column name.
     * @param columnType Java type of the column.
     *
     * @return this, for chaining.
     *
     * @see com.google.appengine.api.datastore.PropertyProjection
     */
    public QueryBuilder addProjection(String column, Class columnType) {
        this.projections.put(column, columnType);
        this.query.addProjection(new PropertyProjection(column, columnType));
        return this;
    }

    /**
     * Add multiple filters to the query.
     *
     * @param filterClauseQueue filter clause queue from {@link com.impetus.kundera.query.KunderaQuery}.
     *
     * @return this, for chaining.
     *
     * @see com.impetus.kundera.query.KunderaQuery.FilterClause
     * @see com.google.appengine.api.datastore.Query.Filter
     */
    public QueryBuilder addFilters(Queue filterClauseQueue) {
        boolean isComposite = false;
        String composeOperator = null;
        Query.Filter previousFilter = null;

        for (Object filterClause : filterClauseQueue) {
            if (filterClause instanceof KunderaQuery.FilterClause) {
                Query.Filter propertyFilter = generatePropertyFilter((KunderaQuery.FilterClause) filterClause);
                if (!isComposite) {
                    addFilter(propertyFilter);
                } else {
                    propertyFilter = composeFilter(propertyFilter, composeOperator, previousFilter);
                    addFilter(propertyFilter);
                }
                previousFilter = propertyFilter;
            } else if (filterClause instanceof String) {
                isComposite = true;
                composeOperator = filterClause.toString().trim();
            }
        }
        return this;
    }

    /**
     * Add a filters to the query.
     *
     * @param propertyFilter datastore {@link com.google.appengine.api.datastore.Query.Filter}.
     *
     * @return this, for chaining.
     *
     * @see com.google.appengine.api.datastore.Query.Filter
     */
    public QueryBuilder addFilter(Query.Filter propertyFilter) {
        this.query.setFilter(propertyFilter);
        return this;
    }

    /**
     * Add orderings to the query.
     *
     * @param ordering list of {@link com.impetus.kundera.query.KunderaQuery.SortOrdering}.
     *
     * @return this, for chaining.
     *
     * @see com.impetus.kundera.query.KunderaQuery.SortOrder
     * @see com.google.appengine.api.datastore.Query.SortDirection
     */
    public QueryBuilder addOrderings(List<KunderaQuery.SortOrdering> ordering) {
        if (ordering != null && !ordering.isEmpty()) {
            for (KunderaQuery.SortOrdering order : ordering) {
                Query.SortDirection direction = parseOrdering(order.getOrder());
                try {
                    String attributeName = order.getColumnName().split("\\.")[1];
                    Attribute attribute = entityType.getAttribute(attributeName);
                    String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
                    addOrdering(direction, jpaColumnName);
                } catch (IndexOutOfBoundsException iobex) {
                    /* case fail in split() */
                    throw new KunderaException("Attribute [" + order.getColumnName() + "] " +
                            "not found in entity class: " + getEntityClass());
                } catch (NullPointerException npe) {
                    /* case attribute not found */
                    throw new KunderaException("Attribute [" + order.getColumnName() + "] " +
                            "not found in entity class: " + getEntityClass());
                }
            }
        }
        return this;
    }

    /**
     * Add an order to the query.
     *
     * @param direction     datastore {@link com.google.appengine.api.datastore.Query.SortDirection}.
     * @param jpaColumnName jpa name mapping for the column.
     *
     * @return this, for chaining.
     *
     * @see com.google.appengine.api.datastore.Query.SortDirection
     */
    private QueryBuilder addOrdering(Query.SortDirection direction, String jpaColumnName) {
        this.query.addSort(jpaColumnName, direction);
        return this;
    }

    private Query.Filter composeFilter(Query.Filter propertyFilter, String composeOperator, Query.Filter previousFilter) {
        if (composeOperator.equalsIgnoreCase("AND")) {
            return Query.CompositeFilterOperator.and(previousFilter,
                    propertyFilter);
        } else if (composeOperator.equalsIgnoreCase("OR")) {
            return Query.CompositeFilterOperator.or(previousFilter,
                    propertyFilter);
        }
        throw new KunderaException("Composition with [" + composeOperator + "] not supported by Datastore");
    }

    private Query.Filter generatePropertyFilter(KunderaQuery.FilterClause filterClause) {
        Object filterValue = filterClause.getValue().get(0);
        Query.FilterOperator operator = parseCondition(filterClause.getCondition());
        String property = filterClause.getProperty();

        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        String filedName = entityMetadata.getFieldName(property);
        if (entityType.getAttribute(filedName).isAssociation()) {
            /* filter on related entity */
            Relation relation = entityMetadata.getRelation(filedName);
            String targetKind = relation.getTargetEntity().getSimpleName();
            Key key = DatastoreUtils.createKey(targetKind, filterValue);
            return new Query.FilterPredicate(property, operator, key);
        } else if (property.equals(idColumnName)) {
            /* filter on entity ID */
            Key key = DatastoreUtils.createKey(this.kind, filterValue);
            return new Query.FilterPredicate(Entity.KEY_RESERVED_PROPERTY, operator, key);
        } else {
            /* filter on entity filed */
            return new Query.FilterPredicate(property, operator, filterValue);
        }
    }

    private Query.FilterOperator parseCondition(String condition) {
        if (condition.equals("=")) {
            return Query.FilterOperator.EQUAL;
        } else if (condition.equals("!=")) {
            return Query.FilterOperator.NOT_EQUAL;
        } else if (condition.equals(">")) {
            return Query.FilterOperator.GREATER_THAN;
        } else if (condition.equals(">=")) {
            return Query.FilterOperator.GREATER_THAN_OR_EQUAL;
        } else if (condition.equalsIgnoreCase("IN")) {
            return Query.FilterOperator.IN;
        } else if (condition.equals("<")) {
            return Query.FilterOperator.LESS_THAN;
        } else if (condition.equals("<=")) {
            return Query.FilterOperator.LESS_THAN_OR_EQUAL;
        }
        throw new KunderaException("Condition [" + condition + "] not supported by Datastore");
    }

    private Query.SortDirection parseOrdering(KunderaQuery.SortOrder order) {
        if (order.equals(KunderaQuery.SortOrder.ASC)) {
            return Query.SortDirection.ASCENDING;
        } else if (order.equals(KunderaQuery.SortOrder.DESC)) {
            return Query.SortDirection.DESCENDING;
        }
        throw new KunderaException("Ordering [" + order + "] not supported by Datastore");
    }
}
