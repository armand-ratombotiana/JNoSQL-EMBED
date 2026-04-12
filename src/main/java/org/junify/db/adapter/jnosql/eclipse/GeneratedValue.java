package org.junify.db.eclipse;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface GeneratedValue {
    String strategy() default "UUID";
}
