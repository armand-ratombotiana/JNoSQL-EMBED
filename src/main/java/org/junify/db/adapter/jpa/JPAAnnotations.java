package org.junify.db.adapter.jpa;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String name() default "";
    boolean nullable() default true;
    int length() default 255;
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    String name() default "";
    String schema() default "";
    String catalog() default "";
    public abstract String[] uniqueConstraints() default {};
    public abstract Index[] indexes() default {};
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeneratedValue {
    Strategy strategy() default Strategy.AUTO;
    
    enum Strategy {
        AUTO,
        IDENTITY,
        SEQUENCE,
        TABLE
    }
}

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Temporal {
    TemporalType value() default TemporalType.TIMESTAMP;
    
    enum TemporalType {
        DATE,
        TIME,
        TIMESTAMP
    }
}
