package org.junify.db.adapter.jpa;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.Repeatable;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Entity {
    String name() default "";
    String table() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Embeddable {
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MappedSuperclass {
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface IdClass {
    Class value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DiscriminatorColumn {
    String name() default "";
    DiscriminatorType discriminatorType() default DiscriminatorType.STRING;
    String columnDefinition() default "";
    int length() default 31;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DiscriminatorValue {
    String value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Inheritance {
    InheritanceType strategy() default InheritanceType.SINGLE_TABLE;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SecondaryTable {
    String name() default "";
    String catalog() default "";
    String schema() default "";
    PrimaryKeyJoinColumn[] pkJoinColumns() default {};
    UniqueConstraint[] uniqueConstraints() default {};
    Index[] indexes() default {};
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EmbeddedId {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transient {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Basic {
    FetchType fetch() default FetchType.EAGER;
    boolean optional() default true;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BasicColumn {
    String name() default "";
    int length() default 255;
    boolean nullable() default true;
    boolean unique() default false;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    String name() default "";
    String schema() default "";
    String catalog() default "";
    UniqueConstraint[] uniqueConstraints() default {};
    Index[] indexes() default {};
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String name() default "";
    boolean unique() default false;
    boolean nullable() default true;
    boolean insertable() default true;
    boolean updatable() default true;
    String columnDefinition() default "";
    String table() default "";
    int length() default 255;
    int precision() default 0;
    int scale() default 0;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Temporal {
    TemporalType value() default TemporalType.TIMESTAMP;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeneratedValue {
    GenerationType strategy() default GenerationType.AUTO;
    String generator() default "";
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SequenceGenerator {
    String name() default "";
    String sequenceName() default "";
    int initialValue() default 1;
    int allocationSize() default 50;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TableGenerator {
    String name() default "";
    String table() default "";
    String catalog() default "";
    String schema() default "";
    String pkColumnName() default "";
    String valueColumnName() default "";
    String pkColumnValue() default "";
    int initialValue() default 1;
    int allocationSize() default 50;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinColumn {
    String name() default "";
    String referencedColumnName() default "";
    String table() default "";
    boolean unique() default false;
    boolean nullable() default true;
    boolean insertable() default true;
    boolean updatable() default true;
    String columnDefinition() default "";
    ForeignKey foreignKey() default @ForeignKey;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinTable {
    String name() default "";
    String catalog() default "";
    String schema() default "";
    JoinColumn[] joinColumns() default {};
    JoinColumn[] inverseJoinColumns() default {};
}

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToOne {
    Class<?> targetEntity() default void.class;
    CascadeType[] cascade() default {};
    FetchType fetch() default FetchType.EAGER;
    String mappedBy() default "";
    boolean optional() default true;
    @JoinColumn referencedColumnName() default @JoinColumn;
}

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToMany {
    Class<?> targetEntity() default void.class;
    CascadeType[] cascade() default {};
    FetchType fetch() default FetchType.LAZY;
    String mappedBy() default "";
    @OrderBy value() default "";
}

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ManyToOne {
    Class<?> targetEntity() default void.class;
    CascadeType[] cascade() default {};
    FetchType fetch() default FetchType.EAGER;
    boolean optional() default true;
    @JoinColumn referencedColumnName() default @JoinColumn;
}

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ManyToMany {
    Class<?> targetEntity() default void.class;
    CascadeType[] cascade() default {};
    FetchType fetch() default FetchType.LAZY;
    String mappedBy() default "";
    JoinTable table() default @JoinTable;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PrimaryKeyJoinColumn {
    String name() default "";
    String referencedColumnName() default "";
    String columnDefinition() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OrderBy {
    String value() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OrderColumn {
    String name() default "";
    boolean nullable() default true;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedQueries {
    NamedQuery[] value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(NamedQueries.class)
public @interface NamedQuery {
    String name();
    String query();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedNativeQuery {
    String name();
    String query();
    QueryHint[] hints() default {};
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SqlResultSetMapping {
    String name();
    EntityResult[] entities() default {};
    ColumnResult[] columns() default {};
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EntityResult {
    String entityClass();
    String name() default "";
    FieldResult[] fields() default {};
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FieldResult {
    String name();
    String column();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnResult {
    String name();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedEntityGraph {
    String name();
    NamedAttributeNode[] attributeNodes() default {};
    boolean subgraphByDefault() default false;
}

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(NamedAttributeNodes.class)
public @interface NamedAttributeNode {
    String value();
    String subgraph() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(NamedEntityGraphs.class)
public @interface NamedEntityGraphs {
    NamedEntityGraph[] value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedSubgraph {
    String name();
    Class type();
    NamedAttributeNode[] attributeNodes() default {};
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface UniqueConstraint {
    String name() default "";
    String[] columnNames();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    String name() default "";
    String columnList() default "";
    boolean unique() default false;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ForeignKey {
    String name() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AttributeOverride {
    String name();
    Column column();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AttributeOverrides {
    AttributeOverride[] value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AssociationOverride {
    String name();
    JoinColumn joinColumn();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AssociationOverrides {
    AssociationOverride[] value();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Version {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Lob {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Formula {
    String value();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PostPersist {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PostRemove {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PrePersist {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PreRemove {
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Access {
    AccessType value() default AccessType.FIELD;
}

public enum TemporalType {
    DATE, TIME, TIMESTAMP
}

public enum GenerationType {
    AUTO, IDENTITY, SEQUENCE, TABLE
}

public enum FetchType {
    LAZY, EAGER
}

public enum CascadeType {
    ALL, PERSIST, MERGE, REMOVE, REFRESH, DETACH
}

public enum InheritanceType {
    SINGLE_TABLE, JOINED, TABLE_PER_CLASS
}

public enum DiscriminatorType {
    STRING, CHAR, INTEGER
}

public enum AccessType {
    FIELD, PROPERTY
}

public @interface QueryHint {
    String name();
    String value();
}