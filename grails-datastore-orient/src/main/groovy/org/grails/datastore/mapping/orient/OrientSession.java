package org.grails.datastore.mapping.orient;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.grails.datastore.mapping.core.AbstractSession;
import org.grails.datastore.mapping.document.config.DocumentMappingContext;
import org.grails.datastore.mapping.engine.Persister;
import org.grails.datastore.mapping.model.MappingContext;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.orient.engine.OrientEntityPersister;
import org.grails.datastore.mapping.transactions.SessionOnlyTransaction;
import org.grails.datastore.mapping.transactions.Transaction;
import org.springframework.context.ApplicationEventPublisher;

import java.io.Serializable;

/**
 * A {@link org.grails.datastore.mapping.core.Session} implementation for the Orient document store.
 *
 * @author Emrul Islam
 */
public class OrientSession extends AbstractSession<ODatabaseDocumentTx> {
    ODatabase orientDatabaseService;

    public OrientSession(OrientDatastore datastore, MappingContext mappingContext, ODatabase orientDatabaseService, ApplicationEventPublisher publisher) {
        super(datastore, mappingContext, publisher, false);
        this.orientDatabaseService = orientDatabaseService;
    }


    public ODatabaseDocumentTx getNativeInterface() {
        return (ODatabaseDocumentTx)orientDatabaseService;
    }

    @Override
    protected Persister createPersister(Class cls, MappingContext mappingContext) {
        final PersistentEntity entity = mappingContext.getPersistentEntity(cls.getName());
        return entity == null ? null : new OrientEntityPersister(mappingContext, entity, this, publisher);
    }

    @Override
    protected Transaction beginTransactionInternal() {
        return new SessionOnlyTransaction<ODatabaseDocumentTx>(getNativeInterface(), this);
    }

    @Override
    public void cacheEntry(PersistentEntity entity, Serializable key, Object entry) {
        super.cacheEntry(entity.getRootEntity(), key, entry);
    }
}
