package org.junify.db.micronaut;

import org.junify.db.JunifyDB;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Factory that creates typed {@link JunifyDBMicronautRepository} instances
 * for a given entity class. Inject this bean in Micronaut components.
 */
@Singleton
public class JunifyDBRepositoryFactory {

    @Inject
    JunifyDB junifyDB;

    public <T, ID> JunifyDBMicronautRepository<T, ID> createRepository(
            Class<T> entityClass, Class<ID> idClass) {
        return new JunifyDBMicronautRepository<>(junifyDB, entityClass, idClass);
    }
}
