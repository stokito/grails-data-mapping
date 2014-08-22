package org.grails.datastore.mapping.orient.config;

import org.grails.datastore.mapping.config.AbstractGormMappingFactory;
import org.grails.datastore.mapping.model.ClassMapping;
import org.grails.datastore.mapping.model.IdentityMapping;
import org.grails.datastore.mapping.model.MappingContext;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.model.types.Identity;

import java.beans.PropertyDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: emrul
 * Date: 11/08/13
 * Time: 13:06
 * To change this template use File | Settings | File Templates.
 */
public class OrientDocumentMappingFactory extends AbstractGormMappingFactory<OrientClass, OrientAttribute> {
    @Override
    protected Class<OrientAttribute> getPropertyMappedFormType() {
        return OrientAttribute.class;
    }

    @Override
    protected Class<OrientClass> getEntityMappedFormType() {
        return OrientClass.class;
    }

    @Override
    public OrientClass createMappedForm(PersistentEntity entity) {
        OrientClass mappedForm = super.createMappedForm(entity);
        mappedForm.setClassName(entity.getName());
        return mappedForm;
    }

    @Override
    public boolean isSimpleType(Class propType) {
        if (propType == null) return false;
        if (propType.isArray()) {
            return isSimpleType(propType.getComponentType()) || super.isSimpleType(propType);
        }
        return OrientMappingContext.isOrientNativeType(propType) || super.isSimpleType(propType);
    }

}
