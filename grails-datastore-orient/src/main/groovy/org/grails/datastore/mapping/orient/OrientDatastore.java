package org.grails.datastore.mapping.orient;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
//import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.grails.datastore.mapping.core.AbstractDatastore;
import org.grails.datastore.mapping.core.Session;
import org.grails.datastore.mapping.document.DocumentDatastore;
import org.grails.datastore.mapping.model.MappingContext;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.converter.Converter;
import java.util.Map;
import static org.grails.datastore.mapping.config.utils.ConfigUtils.read;

/**
 * A Datastore implementation for OrientDB.
 *
 * @author Emrul Islam
 * @since 0.1
 */
public class OrientDatastore extends AbstractDatastore implements InitializingBean, MappingContext.Listener, DisposableBean, DocumentDatastore {

    //protected ODatabase orientDatabaseService;
    protected ODatabasePoolBase pool;


    public static final String CONFIG_USERNAME = "username";
    public static final String CONFIG_PASSWORD = "password";
    public static final String CONFIG_ORIENT_URL = "url";
    public static final String CONFIG_ORIENTDB_TYPE = "type";
    //public static final String CONFIG_DB_POOL_MIN = "minPool";
    //public static final String CONFIG_DB_POOL_MAX = "maxPool";

    public static final String DEFAULT_USERNAME = "admin";
    public static final String DEFAULT_PASSWORD = "admin";
    public static final String DEFAULT_ORIENT_URL = "memory:test";
    public static final String DEFAULT_ORIENTDB_TYPE = ODatabaseDocument.TYPE;

    //public static final int DEFAULT_DB_POOL_MIN = OGlobalConfiguration.DB_POOL_MIN.getValueAsInteger();
    //public static final int DEFAULT_DB_POOL_MAX = OGlobalConfiguration.DB_POOL_MAX.getValueAsInteger();

    private String username = DEFAULT_USERNAME;
    private String password = DEFAULT_PASSWORD;
    private String url = DEFAULT_ORIENT_URL;
    /**
     * dbType may be one of ODatabaseDocument.Type ("document"), OGraphDatabase.Type ("graph").
     * "document" is the default (and only supported for now...)
     */


    private String dbType = DEFAULT_ORIENTDB_TYPE;

    //private int minPoolSize = DEFAULT_DB_POOL_MIN;
    //private int maxPoolSize = DEFAULT_DB_POOL_MAX;

    //com.orientechnologies.orient.

    public OrientDatastore(MappingContext mappingContext) {
        this(mappingContext, null, null);
    }

    public OrientDatastore(MappingContext mappingContext, Map<String, String> connectionDetails,
                           ConfigurableApplicationContext ctx) {
        this(mappingContext, null, connectionDetails, ctx);
    }

    public OrientDatastore(MappingContext mappingContext, ODatabasePoolBase pool, Map<String, String> connectionDetails,
                           ConfigurableApplicationContext ctx) {
        super(mappingContext, connectionDetails, ctx);

        if (connectionDetails != null) {
            username = read(String.class, CONFIG_USERNAME, connectionDetails, DEFAULT_USERNAME);
            password = read(String.class, CONFIG_PASSWORD, connectionDetails, DEFAULT_PASSWORD);
            url = read(String.class, CONFIG_ORIENT_URL, connectionDetails, DEFAULT_ORIENT_URL);
            dbType = read(String.class, CONFIG_ORIENTDB_TYPE, connectionDetails, DEFAULT_ORIENTDB_TYPE);
            //pooled = read(Boolean.class, CONFIG_POOLED, connectionDetails, DEFAULT_POOLED);
            //minPoolSize = read(Integer.class, CONFIG_DB_POOL_MIN, connectionDetails, DEFAULT_DB_POOL_MIN);
            //maxPoolSize = read(Integer.class, CONFIG_DB_POOL_MAX, connectionDetails, DEFAULT_DB_POOL_MAX);
        }

        if ( pool != null ) {
            this.pool = pool;
        }
        /*
        if (pooled && pool == null) {
            switch ( dbType ) {
                case OGraphDatabase.TYPE:
                    this.pool = new OGraphDatabasePool();
                    break;
                case ODatabaseDocument.TYPE:
                default:
                    this.pool = new ODatabaseDocumentPool();
                    break;
            }
            this.pool.setup(minPoolSize, maxPoolSize);
        }*/

        if (mappingContext != null) {
            mappingContext.addMappingContextListener(this);
        }

        initializeConverters(mappingContext);

        mappingContext.getConverterRegistry().addConverter(new Converter<ODocument, ORecordId>() {
            public ORecordId convert(ODocument source) {
                return new ORecordId(source.getIdentity());
            }
        });
    }

    private ODatabasePoolBase getPool() {
        return this.pool;
    }

    private ODatabase getDatabase() {
        //if ( dbType == OGraphDatabase.TYPE ) {
            //return new OGraphDatabase(url);
        //}
        //else {
            return new ODatabaseDocumentTx(url);
        //}
    }

    @Override
    public OrientSession connect() {
        return (OrientSession)connect(connectionDetails);
    }
    @Override
    protected Session createSession(Map<String, String> connectionDetails) {
        ODatabase orientDatabaseService;

        if (this.pool != null) {
            orientDatabaseService = getPool().acquire(url, username, password);
        }
        else {
            orientDatabaseService = getDatabase().open(username, password);
        }
        return new OrientSession(this, getMappingContext(), orientDatabaseService, getApplicationEventPublisher());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void persistentEntityAdded(PersistentEntity entity) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /*
    @Override
    public void afterPropertiesSet() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void persistentEntityAdded(PersistentEntity entity) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();

        if (pool != null) {
            pool.close();
        }
    }
    */
}
