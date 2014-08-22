package org.grails.datastore.gorm

import grails.gorm.tests.GormDatastoreSpec
import org.grails.datastore.gorm.events.AutoTimestampEventListener
import org.grails.datastore.gorm.events.DomainEventListener
import org.grails.datastore.gorm.orient.OrientGormEnhancer
import org.grails.datastore.mapping.core.Session
import org.grails.datastore.mapping.model.MappingContext
import org.grails.datastore.mapping.model.PersistentEntity
import org.grails.datastore.mapping.orient.OrientDatastore
import org.grails.datastore.mapping.orient.OrientSession
import org.grails.datastore.mapping.orient.config.OrientMappingContext
import org.grails.datastore.mapping.transactions.DatastoreTransactionManager
import org.springframework.context.support.GenericApplicationContext
import org.springframework.util.StringUtils
import org.springframework.validation.Errors
import org.springframework.validation.Validator

class Setup {
    /*

    */
    def static connectionDetails = [username: "test", password: "test", url: "remote:192.168.1.116/test"]

    static OrientDatastore orient
    static OrientSession session
    static destroy() {
        orient?.destroy()
    }

    static Session setup(classes) {
        orient = new OrientDatastore(new OrientMappingContext(System.getProperty(GormDatastoreSpec.CURRENT_TEST_NAME) ?: 'test'), connectionDetails, null)
        def ctx = new GenericApplicationContext()
        ctx.refresh()
        orient.setApplicationContext(ctx)
        orient.afterPropertiesSet()

        for (cls in classes) {
            orient.mappingContext.addPersistentEntity(cls)
        }

        PersistentEntity entity = orient.mappingContext.persistentEntities.find { PersistentEntity e -> e.name.contains("TestEntity")}

        orient.mappingContext.addEntityValidator(entity, [
            supports: { Class c -> true },
            validate: { Object o, Errors errors ->
                if (!StringUtils.hasText(o.name)) {
                    errors.rejectValue("name", "name.is.blank")
                }
            }
        ] as Validator)

        def enhancer = new OrientGormEnhancer(orient, new DatastoreTransactionManager(datastore: orient))
        enhancer.enhance()

        orient.mappingContext.addMappingContextListener({ e ->
            enhancer.enhance e
        } as MappingContext.Listener)

        orient.applicationContext.addApplicationListener new DomainEventListener(orient)
        orient.applicationContext.addApplicationListener new AutoTimestampEventListener(orient)

        session = orient.connect()
        return session
    }
}
