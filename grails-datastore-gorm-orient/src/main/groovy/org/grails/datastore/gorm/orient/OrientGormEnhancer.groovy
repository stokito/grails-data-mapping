package org.grails.datastore.gorm.orient

import com.orientechnologies.orient.core.record.impl.ODocument
import org.grails.datastore.gorm.finders.DynamicFinder
import org.grails.datastore.gorm.finders.FinderMethod
import org.grails.datastore.gorm.GormEnhancer
import org.grails.datastore.gorm.GormInstanceApi
import org.grails.datastore.gorm.GormStaticApi
import org.grails.datastore.mapping.core.Datastore
import org.grails.datastore.mapping.core.Session
import org.grails.datastore.mapping.core.SessionCallback
import org.grails.datastore.mapping.core.SessionImplementor
import org.grails.datastore.mapping.orient.OrientSession
import org.grails.datastore.mapping.orient.engine.OrientEntityPersister
import org.grails.datastore.mapping.orient.OrientDatastore
import org.springframework.transaction.PlatformTransactionManager

/**
 * GORM enhancer for Orient.
 *
 * @author Graeme Rocher
 */
class OrientGormEnhancer extends GormEnhancer {

    OrientGormEnhancer(Datastore datastore, PlatformTransactionManager transactionManager) {
        super(datastore, transactionManager)

        //DynamicFinder.registerNewMethodExpression(Near)
    }

    OrientGormEnhancer(Datastore datastore) {
        this(datastore, null)
    }

    protected <D> GormStaticApi<D> getStaticApi(Class<D> cls) {
        return new OrientGormStaticApi<D>(cls, datastore, getFinders())
    }

    protected <D> GormInstanceApi<D> getInstanceApi(Class<D> cls) {
        final api = new OrientGormInstanceApi<D>(cls, datastore)
        api.failOnError = failOnError
        return api
    }
}

class OrientGormInstanceApi<D> extends GormInstanceApi<D> {

    OrientGormInstanceApi(Class<D> persistentClass, Datastore datastore) {
        super(persistentClass, datastore)
    }

    /**
     * Allows accessing to dynamic properties with the dot operator
     *
     * @param instance The instance
     * @param name The property name
     * @return The property value
     */
    def propertyMissing(D instance, String name) {
        getAt(instance, name)
    }

    /**
     * Allows setting a dynamic property via the dot operator
     * @param instance The instance
     * @param name The property name
     * @param val The value
     */
    def propertyMissing(D instance, String name, val) {
        putAt(instance, name, val)
    }
    /**
     * Allows subscript access to schemaless attributes.
     *
     * @param instance The instance
     * @param name The name of the field
     */
    void putAt(D instance, String name, value) {
        if (instance.hasProperty(name)) {
            instance.setProperty(name, value)
        }
        else {
            execute (new SessionCallback<ODocument>() {
                ODocument doInSession(Session session) {
                    SessionImplementor si = (SessionImplementor)session
                    final dbo = getDbo(instance)
                    dbo?.put name, value
                    return dbo
                }
            })

        }
    }

    /**
     * Allows subscript access to schemaless attributes.
     *
     * @param instance The instance
     * @param name The name of the field
     * @return the value
     */
    def getAt(D instance, String name) {
        if (instance.hasProperty(name)) {
            return instance.getProperty(name)
        }

        def dbo = getDbo(instance)
        if (dbo != null && dbo.containsField(name)) {
            return dbo.get(name)
        }
        return null
    }

    /**
     * Return the ODocument instance for the entity
     *
     * @param instance The instance
     * @return The ODocument instance
     */
    ODocument getDbo(D instance) {
        execute (new SessionCallback<ODocument>() {
            ODocument doInSession(Session session) {

                if (!session.contains(instance) && !instance.save()) {
                    throw new IllegalStateException(
                        "Cannot obtain ODocument for transient instance, save a valid instance first")
                }

                OrientEntityPersister persister = session.getPersister(instance)
                def id = persister.getObjectIdentifier(instance)
                def dbo = session.getCachedEntry(persister.getPersistentEntity(), id)
                if (dbo == null) {
                    dbo = persister.getOrientDb().load(id)

                }
                return dbo
            }
        })
    }
}

class OrientGormStaticApi<D> extends GormStaticApi<D> {

    OrientGormStaticApi(Class<D> persistentClass, Datastore datastore, List<FinderMethod> finders) {
        super(persistentClass, datastore, finders)
    }

    @Override
    OrientCriteriaBuilder createCriteria() {
        return new OrientCriteriaBuilder(persistentClass, datastore.currentSession)
    }

}