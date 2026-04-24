package org.junify.db.adapter.jnosql;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.Repeatable;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Entity {
    String value() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
    String value() default "";
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String value() default "";
    boolean updatable() default true;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Parent {
    boolean optional() default false;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Embed {
    String prefix() default "";
    String suffix() default "";
    String separator() default ".";
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transformed {
    Transformer transformer() default @Transformer;
    Class<?> type() default void.class;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Converters.class)
public @interface Converter {
    String from();
    String to();
    Class<?> valueConverter();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Converters {
    Converter[] value();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DisableUpgrade {
    boolean value() default true;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface View {
    ViewscType value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DiscriminatorColumn {
    String name() default "type";
    DiscriminatorStrategy strategy() default DiscriminatorStrategy.CLASS_NAME;
    String separator() default ":";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DiscriminatorValue {
    String value();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TypeKey {
    String value() default "";
    Class<?> targetType() default void.class;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MappedSuperclass {
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface InheritConfiguration {
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Abstract {
    String value() default "";
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    String value();
    boolean unique() default false;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Indexes.class)
public @interface Indexes {
    Index[] value();
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FullText {
    boolean value() default true;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Field {
    String value() default "";
    FieldType type() default FieldType.STRING;
    int maxLength() default 255;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Subdocument {
    String value() default "";
    int maxElements() default 100;
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface IdPrefix {
    int value() default 10;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Factory {
    String value() default "";
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeneratedValue {
    GeneratorStrategy strategy() default GeneratorStrategy.UUID;
    String generator() default "";
    int value() default 64;
    String prefix() default "";
    String suffix() default "";
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Sequence {
    int value() default 1;
    int allocationSize() default 1;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NamedQuery {
    String value();
    String query();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Queries {
    NamedQuery[] value();
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface FieldEvaluation {
    boolean isEquals() default true;
    boolean isGreaterOrEquals() default false;
    boolean isLessOrEquals() default false;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Filter {
    String value();
    String field() default "";
    String condition() default "";
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Filters {
    Filter[] value();
}

public enum GeneratorStrategy {
    UUID, AUTO_INCREMENT, SEQUENCE, CUSTOM
}

public enum DiscriminatorStrategy {
    CLASS_NAME, SIMPLE_NAME, FIELD_VALUE
}

public enum ViewscType {
    ANY, DEFAULT, COLLATION, MOBanLE_CONFIGURATION
}

public enum FieldType {
    STRING, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, DATE, TIME, DATETIME, BINARY
}

public @interface Transformer {
    Class<?> from();
    Class<?> to();
}