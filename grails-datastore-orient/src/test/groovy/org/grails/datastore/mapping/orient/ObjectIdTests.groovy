package org.grails.datastore.mapping.orient

import com.orientechnologies.orient.core.id.ORecordId
import org.junit.Test

class ObjectIdTests extends AbstractOrientTest {

    @Test
    void testBasicPersistenceOperations() {
        ds.mappingContext.addPersistentEntity(ORecordId)

        OrientSession session = ds.connect()

        def te = new OrientRecordIdEntity(name:"Bob")

        session.persist te
        session.flush()

        assert te != null
        assert te.id != null
        assert te.id instanceof ORecordId

        session.clear()
        te = session.retrieve(OrientRecordIdEntity, te.id)

        assert te != null
        assert te.name == "Bob"

        te.name = "Fred"
        session.persist(te)
        session.flush()
        session.clear()

        te = session.retrieve(OrientRecordIdEntity, te.id)
        assert te != null
        assert te.id != null
        assert te.name == 'Fred'

        session.delete te
        session.flush()

        te = session.retrieve(OrientRecordIdEntity, te.id)
        assert te == null
    }
}

class OrientRecordIdEntity {
    ORecordId id
    String name
}
