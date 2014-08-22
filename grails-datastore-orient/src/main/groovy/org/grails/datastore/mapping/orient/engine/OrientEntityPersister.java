package org.grails.datastore.mapping.orient.engine;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.grails.datastore.mapping.core.Session;
import org.grails.datastore.mapping.core.SessionImplementor;
import org.grails.datastore.mapping.dirty.checking.DirtyCheckable;
import org.grails.datastore.mapping.engine.AssociationIndexer;
import org.grails.datastore.mapping.engine.EntityAccess;
import org.grails.datastore.mapping.engine.NativeEntryEntityPersister;
import org.grails.datastore.mapping.engine.PropertyValueIndexer;
import org.grails.datastore.mapping.model.MappingContext;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.model.PersistentProperty;
import org.grails.datastore.mapping.model.types.*;
import org.grails.datastore.mapping.orient.OrientSession;
import org.grails.datastore.mapping.query.Query;
import org.springframework.context.ApplicationEventPublisher;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: emrul
 * Date: 12/08/13
 * Time: 21:47
 * To change this template use File | Settings | File Templates.
 */
public class OrientEntityPersister extends NativeEntryEntityPersister<ODocument, Object> {

    public OrientEntityPersister(MappingContext mappingContext, PersistentEntity entity, Session session, ApplicationEventPublisher publisher) {
        super(mappingContext, entity, session, publisher);
    }



    public OrientSession getOrientSession() {
        return (OrientSession)getSession();
    }

    protected ODatabaseDocumentTx getOrientDb() {
        return (ODatabaseDocumentTx)getOrientSession().getNativeInterface();
    }

    protected ORecordId createRecordIdWithKey(Object key) {
        ORecordId recId = null;

        if ( key instanceof ORecordId ) {
            recId = (ORecordId)key;
        }
        else if ( key instanceof String ) {
            recId = new ORecordId((String)key);
        }
        return recId;
    }

    @Override
    public String getEntityFamily() {
        return this.getPersistentEntity().getName();
    }

    @Override
    protected void deleteEntry(String family, Object key, Object entry) {
        getOrientDb().delete(createRecordIdWithKey(key));
    }

    @Override
    protected Object generateIdentifier(PersistentEntity persistentEntity, ODocument entry) {
        //PersistentEntity root = persistentEntity.getRootEntity();
        //return persistentEntity.getName() + ":" + Integer.toString(entry.hashCode());  //To change body of implemented methods use File | Settings | File Templates.
        return entry.hashCode();
    }

    @Override
    public PropertyValueIndexer getPropertyIndexer(PersistentProperty property) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public AssociationIndexer getAssociationIndexer(ODocument nativeEntry, Association association) {
        return new OrientAssociationIndexer(nativeEntry, association, (OrientSession) session);
    }

    @Override
    protected ODocument createNewEntry(String family) {
        return new ODocument(family);
    }

    @Override
    protected Object getEntryValue(ODocument nativeEntry, String property) {
        return nativeEntry.field(property);
    }

    @Override
    protected void setEntryValue(ODocument nativeEntry, String key, Object value) {
        nativeEntry.field(key, value);
    }

    @Override
    protected ODocument retrieveEntry(PersistentEntity persistentEntity, String family, Serializable key) {
        return getOrientDb().load(createRecordIdWithKey(key));
    }

    @Override
    protected Object storeEntry(PersistentEntity persistentEntity, EntityAccess entityAccess, Object storeId, ODocument nativeEntry) {
        nativeEntry.setClassName(persistentEntity.getName());
        return getOrientDb().save(nativeEntry).getIdentity();
    }

    @Override
    protected void updateEntry(PersistentEntity persistentEntity, EntityAccess entityAccess, Object key, ODocument entry) {
        getOrientDb().save(entry);
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void deleteEntries(String family, List<Object> keys) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Query createQuery() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Formulates a database reference for the given entity, association and association id
     *
     * @param persistentEntity The entity being persisted
     * @param association The association
     * @param associationId The association id
     * @return A database reference
     */
    @Override
    protected Object formulateDatabaseReference(PersistentEntity persistentEntity, Association association, Serializable associationId) {
        SessionImplementor<Object> si = (SessionImplementor<Object>) session;
        if ( associationId instanceof Integer) {
            Object assocObj = si.getCachedEntry( association.getAssociatedEntity().getRootEntity(), associationId);
            if (assocObj != null) {
                return assocObj;
            }
        }
        /*
        if ( associationId instanceof String) {
            String[] assocRef = ((String)associationId).split(":");
            if ( assocRef.length == 2 ) {
                Object assocObj = si.getCachedEntry( getMappingContext().getPersistentEntity(assocRef[0]), associationId);
                if (assocObj != null) {
                    return assocObj;
                }
            }
        }
        */
        return associationId;
    }

/*
    @Override
    protected Serializable persistEntity(PersistentEntity pe, Object obj) {
        if (obj == null) {
            //log.error("obj is null");
            throw new IllegalStateException("obj is null");
        }
        return persistEntity(pe, obj, new HashSet());
    }

    protected Serializable persistEntity(PersistentEntity pe, Object obj, Collection persistingColl ) {

        if (persistingColl.contains(obj)) {
            return null;
        } else {
            persistingColl.add(obj);
        }

        boolean isDirty = obj instanceof DirtyCheckable ? ((DirtyCheckable)obj).hasChanged() : true;

        if (getSession().containsPersistingInstance(obj) && (!isDirty)) {
            return null;
        }

        EntityAccess entityAccess = createEntityAccess(pe, obj);
        if (getMappingContext().getProxyFactory().isProxy(obj)) {
            return (Serializable) entityAccess.getIdentifier();
        }


        getSession().addPersistingInstance(obj);

        // cancel operation if vetoed
        boolean isUpdate = entityAccess.getIdentifier() != null;
        if (isUpdate) {
            if (cancelUpdate(pe, entityAccess)) {
                return null;
            }
            getSession().addPendingUpdate(new NodePendingUpdate(entityAccess, getCypherEngine(), getMappingContext()));
            persistAssociationsOfEntity(pe, entityAccess, true, persistingColl);
            firePostUpdateEvent(pe, entityAccess);

        } else {
            if (cancelInsert(pe, entityAccess)) {
                return null;
            }
            getSession().addPendingInsert(new NodePendingInsert(getSession().getDatastore().nextIdForType(pe), entityAccess, getCypherEngine(), getMappingContext()));
            persistAssociationsOfEntity(pe, entityAccess, false, persistingColl);
            firePostInsertEvent(pe, entityAccess);
        }

        return (Serializable) entityAccess.getIdentifier();
    }

    private void persistAssociationsOfEntity(PersistentEntity pe, EntityAccess entityAccess, boolean isUpdate, Collection persistingColl) {

        Object obj = entityAccess.getEntity();
        DirtyCheckable dirtyCheckable = null;
        if (obj instanceof DirtyCheckable) {
            dirtyCheckable = (DirtyCheckable)obj;
        }

        for (PersistentProperty pp: pe.getAssociations()) {
            if ((!isUpdate) || ((dirtyCheckable!=null) && dirtyCheckable.hasChanged(pp.getName()))) {

                Object propertyValue = entityAccess.getProperty(pp.getName());

                if ((pp instanceof OneToMany) || (pp instanceof ManyToMany)) {
                    Association association = (Association) pp;

                    if (propertyValue!= null) {

                        if (association.isBidirectional()) {  // Populate other side of bidi
                            for (Object associatedObject: (Iterable)propertyValue) {
                                EntityAccess assocEntityAccess = createEntityAccess(association.getAssociatedEntity(), associatedObject);
                                assocEntityAccess.setProperty(association.getReferencedPropertyName(), obj);
                            }
                        }

                        Iterable targets = (Iterable) propertyValue;
                        persistEntities(association.getAssociatedEntity(), targets, persistingColl);

                        boolean reversed = RelationshipUtils.useReversedMappingFor(association);

                        if (!reversed) {
                            if (!(propertyValue instanceof LazyEnititySet)) {
                                LazyEnititySet les = new LazyEnititySet(entityAccess, association, getMappingContext().getProxyFactory(), getSession());
                                les.addAll(targets);
                                entityAccess.setProperty(association.getName(), les);
                            }
                        }
                    }
                } else if (pp instanceof ToOne) {
                    if (propertyValue != null) {
                        ToOne to = (ToOne) pp;

                        if (to.isBidirectional()) {  // Populate other side of bidi
                            EntityAccess assocEntityAccess = createEntityAccess(to.getAssociatedEntity(), propertyValue);
                            if (to instanceof OneToOne) {
                                assocEntityAccess.setProperty(to.getReferencedPropertyName(), obj);
                            } else {
                                Collection collection = (Collection) assocEntityAccess.getProperty(to.getReferencedPropertyName());
                                if (collection == null ) {
                                    collection = new ArrayList();
                                    assocEntityAccess.setProperty(to.getReferencedPropertyName(), collection);
                                }
                                if (!collection.contains(obj)) {
                                    collection.add(obj);
                                }
                            }
                        }

                        persistEntity(to.getAssociatedEntity(), propertyValue, persistingColl);

                        boolean reversed = RelationshipUtils.useReversedMappingFor(to);
                        String relType = RelationshipUtils.relationshipTypeUsedFor(to);

                        if (!reversed) {
                            getSession().addPendingInsert(new RelationshipPendingDelete(entityAccess, relType, null , getCypherEngine()));
                            getSession().addPendingInsert(new RelationshipPendingInsert(entityAccess, relType,
                                    new EntityAccess(to.getAssociatedEntity(), propertyValue),
                                    getCypherEngine()));
                        }


                    }
                } else {
                    throw new IllegalArgumentException("wtf don't know how to handle " + pp + "(" + pp.getClass() +")" );

                }
            }


        }
    }
    */
}
