package org.grails.datastore.mapping.orient.config;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import groovy.lang.Closure;
import org.grails.datastore.mapping.document.config.DocumentMappingContext;
import org.grails.datastore.mapping.model.*;
import java.math.BigDecimal;
import java.util.*;

import org.grails.datastore.mapping.document.config.Collection;


/**
 * Models a {@link org.grails.datastore.mapping.model.MappingContext} for Orient.
 *
 * @author Emrul Islam
 */

@SuppressWarnings("rawtypes")
public class OrientMappingContext extends DocumentMappingContext {
    /**
     * Java types supported as orient property types.
     * See: https://github.com/orientechnologies/orientdb/wiki/Types
     */
    private static final Set<String> ORIENT_NATIVE_TYPES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
            String.class.getName(),
            Byte.class.getName(),
            Short.class.getName(),
            Integer.class.getName(),
            Long.class.getName(),
            Float.class.getName(),
            Double.class.getName(),
            BigDecimal.class.getName(),
            Date.class.getName(),
            byte[].class.getName(),
            ORecordId.class.getName(),
            ORecord.class.getName(),
            ODocument.class.getName()
    )));


    /**
     * Check whether a type is a native orient type that can be stored without conversion.
     * @param clazz The class to check.
     * @return true if no conversion is required and the type can be stored natively.
     */
    public static boolean isOrientNativeType(Class clazz) {
        return  (  OrientMappingContext.ORIENT_NATIVE_TYPES.contains(clazz.getName())
                || ORecord.class.isAssignableFrom(clazz.getClass())
                || OSerializableStream.class.isAssignableFrom(clazz.getClass())
                );
    }


    public OrientMappingContext(String defaultDatabaseName) {
        super(defaultDatabaseName);
    }

    public OrientMappingContext(String defaultDatabaseName, Closure defaultMapping) {
        super(defaultDatabaseName, defaultMapping);
    }


    @Override
    protected MappingFactory createDocumentMappingFactory(Closure defaultMapping) {
        OrientDocumentMappingFactory orientDocumentMappingFactory = new OrientDocumentMappingFactory();
        orientDocumentMappingFactory.setDefaultMapping(defaultMapping);
        return orientDocumentMappingFactory;
    }

    @Override
    public PersistentEntity createEmbeddedEntity(Class type) {
        return new DocumentEmbeddedPersistentEntity(type, this);
    }

    class DocumentEmbeddedPersistentEntity extends EmbeddedPersistentEntity {

        private DocumentCollectionMapping classMapping ;
        public DocumentEmbeddedPersistentEntity(Class type, MappingContext ctx) {
            super(type, ctx);
            classMapping = new DocumentCollectionMapping(this, ctx);
        }

        @Override
        public ClassMapping getMapping() {
            return classMapping;
        }

        public class DocumentCollectionMapping extends AbstractClassMapping<Collection> {
            private Collection mappedForm;

            public DocumentCollectionMapping(PersistentEntity entity, MappingContext context) {
                super(entity, context);
                this.mappedForm = (Collection) context.getMappingFactory().createMappedForm(DocumentEmbeddedPersistentEntity.this);
            }
            @Override
            public Collection getMappedForm() {
                return mappedForm ;
            }
        }
    }
}
