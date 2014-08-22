package org.grails.datastore.mapping.orient.config;

import org.grails.datastore.mapping.config.Entity;
import org.grails.datastore.mapping.document.config.Collection;

/**
 * Configures how an entity is mapped onto a Class collection
 *
 * @author Emrul Islam
 */
public class OrientClass extends Collection {
    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    protected String className;


}
