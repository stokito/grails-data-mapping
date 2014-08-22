package org.grails.datastore.gorm.orient.bean.factory

import com.orientechnologies.orient.core.db.ODatabasePoolBase
import org.grails.datastore.gorm.events.AutoTimestampEventListener
import org.grails.datastore.gorm.events.DomainEventListener
import org.grails.datastore.mapping.model.MappingContext
import org.grails.datastore.mapping.orient.OrientDatastore
import org.springframework.beans.factory.FactoryBean
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware

class OrientDatastoreFactoryBean implements FactoryBean<OrientDatastore>, ApplicationContextAware {

    ODatabasePoolBase pool
    MappingContext mappingContext
    Map<String,String> config = [:]
    ApplicationContext applicationContext

    OrientDatastore getObject() {

        OrientDatastore datastore
        if (pool) {
            datastore = new OrientDatastore(mappingContext, pool, config, applicationContext)
        }
        else {
            datastore = new OrientDatastore(mappingContext, config, applicationContext)
        }

        applicationContext.addApplicationListener new DomainEventListener(datastore)
        applicationContext.addApplicationListener new AutoTimestampEventListener(datastore)

        datastore.afterPropertiesSet()
        datastore
    }

    Class<?> getObjectType() { OrientDatastore }

    boolean isSingleton() { true }
}
