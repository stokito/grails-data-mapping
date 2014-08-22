package org.grails.datastore.mapping.orient

import com.orientechnologies.orient.client.remote.OServerAdmin
import org.grails.datastore.mapping.core.DatastoreUtils
import org.grails.datastore.mapping.core.Session
import org.grails.datastore.mapping.orient.config.OrientMappingContext
import org.junit.After
import org.junit.Before
import org.springframework.context.support.GenericApplicationContext


abstract class AbstractOrientTest {
    protected OrientDatastore ds
    protected Session session

    protected String testDbName = "dstest"
    protected String testDbType = "document"
    protected String testStorageMode = "local"

    protected String username = "root"
    protected String password = "48FC273A8599FD91591BE3BB61EF24AA007AB935FD6949C8173BAF2B63256DFB"
    protected String url = "remote:localhost/"+testDbName


    private OServerAdmin getServerAdmin() {
        return new OServerAdmin(url).connect(username, password)
    }

    @Before
    void setUp() {
        def connectionDetails = [:]
        connectionDetails[OrientDatastore.CONFIG_USERNAME] = username
        connectionDetails[OrientDatastore.CONFIG_PASSWORD] = password
        connectionDetails[OrientDatastore.CONFIG_ORIENT_URL] = url

        def mappingContext = new OrientMappingContext(testDbName);

        OServerAdmin serverAdmin = getServerAdmin()

        if ( serverAdmin.existsDatabase(testDbName) ) {
            serverAdmin.dropDatabase(testDbName)
        }
        serverAdmin.createDatabase(testDbType, testStorageMode)
        serverAdmin.close()

        def ctx = new GenericApplicationContext()
        ctx.refresh()
        ds = new OrientDatastore(mappingContext, connectionDetails, ctx)
        ds.applicationContext = ctx

        session = ds.connect()
        DatastoreUtils.bindSession session
    }

    @After
    void tearDown() {
        session.disconnect()
        ds.destroy()

        /*
        OServerAdmin serverAdmin = getServerAdmin()
        if ( serverAdmin.existsDatabase(testDbName) ) {
            serverAdmin.dropDatabase(testDbName)
        }
        serverAdmin.close()
        */
    }
}