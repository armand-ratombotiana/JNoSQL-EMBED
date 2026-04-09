package org.jnosql.embed.eclipse;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Inherited
public @interface Entity {
    String value() default "";
}
