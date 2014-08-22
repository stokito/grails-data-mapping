package org.grails.datastore.gorm.orient

import grails.gorm.CriteriaBuilder
import org.grails.datastore.mapping.core.Session
import org.grails.datastore.mapping.query.Query

public class OrientCriteriaBuilder extends CriteriaBuilder {

    public OrientCriteriaBuilder(final Class<?> targetClass, final Session session, final Query query) {
        super(targetClass, session, query);
    }

    public OrientCriteriaBuilder(final Class<?> targetClass, final Session session) {
        super(targetClass, session);
    }
}