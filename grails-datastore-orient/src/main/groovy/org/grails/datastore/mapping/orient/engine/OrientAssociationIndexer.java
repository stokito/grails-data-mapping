package org.grails.datastore.mapping.orient.engine;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.grails.datastore.mapping.core.SessionImplementor;
import org.grails.datastore.mapping.engine.AssociationIndexer;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.model.types.Association;
import org.grails.datastore.mapping.orient.OrientSession;
import org.grails.datastore.mapping.query.Query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by emrul on 21/08/14.
 */
public class OrientAssociationIndexer implements AssociationIndexer {
        private ODocument nativeEntry;
        private Association association;
        private OrientSession session;
        private boolean isReference = true;

        public OrientAssociationIndexer(ODocument nativeEntry, Association association, OrientSession session) {
            this.nativeEntry = nativeEntry;
            this.association = association;
            this.session = session;
            //this.isReference = isReference(association);
        }

        public void preIndex(final Object primaryKey, final List foreignKeys) {
            // if the association is a unidirectional one-to-many we store the keys
            // embedded in the owning entity, otherwise we use a foreign key
            //if (!association.isBidirectional()) {
                SessionImplementor<Object> si = (SessionImplementor<Object>) session;
                ODatabaseDocumentTx db = session.getNativeInterface();
                List refs = new ArrayList();
                for (Object foreignKey : foreignKeys) {
                    if ( foreignKey instanceof Integer) {
                        Object assocObj = si.getCachedEntry( association.getAssociatedEntity().getRootEntity(), (Serializable)foreignKey);
                        if (assocObj != null) {
                            refs.add(assocObj);
                        }
                    }
                    //if (isReference) {
                    //    dbRefs.add(new DBRef(db, getCollectionName(association.getAssociatedEntity()), foreignKey));
                    //}
                    //else {

                    //}
                }
                // update the native entry directly.
                nativeEntry.field(association.getName(), refs);
            //}
        }

        public void index(final Object primaryKey, final List foreignKeys) {
            // indexing is handled by putting the data in the native entry before it is persisted, see preIndex above.
        }

        public List query(Object primaryKey) {
            // for a unidirectional one-to-many we use the embedded keys
            if (!association.isBidirectional()) {
                final Object indexed = nativeEntry.field(association.getName());
                if (!(indexed instanceof Collection)) {
                    return Collections.emptyList();
                }
                List indexedList = getIndexedAssociationsAsList(indexed);

                if (associationsAreDbRefs(indexedList)) {
                    return extractIdsFromDbRefs(indexedList);
                }
                return indexedList;
            }
            // for a bidirectional one-to-many we use the foreign key to query the inverse side of the association
            Association inverseSide = association.getInverseSide();
            Query query = session.createQuery(association.getAssociatedEntity().getJavaClass());
            query.eq(inverseSide.getName(), primaryKey);
            query.projections().id();
            return query.list();
        }

        public PersistentEntity getIndexedEntity() {
            return association.getAssociatedEntity();
        }

        public void index(Object primaryKey, Object foreignKey) {
            // TODO: Implement indexing of individual entities
        }

        private List getIndexedAssociationsAsList(Object indexed) {
            return (indexed instanceof List) ? (List) indexed : new ArrayList(((Collection) indexed));
        }

        private boolean associationsAreDbRefs(List indexedList) {
            return false;// !indexedList.isEmpty() && (indexedList.get(0) instanceof DBRef);
        }

        private List extractIdsFromDbRefs(List indexedList) {
            List resolvedDbRefs = new ArrayList();
            for (Object indexedAssociation : indexedList) {
                //resolvedDbRefs.add(((DBRef) indexedAssociation).getId());
            }
            return resolvedDbRefs;
        }
    }
