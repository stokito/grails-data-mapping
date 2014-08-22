package org.grails.datastore.mapping.orient

import org.junit.Test

class StringIdTests extends AbstractOrientTest {

    @Test
    void testBasicPersistenceOperations() {
        ds.mappingContext.addPersistentEntity(OrientStringRecordIdEntity)

        OrientSession session = ds.connect()

        def te = new OrientStringRecordIdEntity (name:"Bob")

        session.persist te
        session.flush()

        assert te != null
        assert te.id != null
        assert te.id instanceof String

        session.clear()
        te = session.retrieve(OrientStringRecordIdEntity, te.id)

        assert te != null
        assert te.name == "Bob"

        te.name = "Fred"
        session.persist(te)
        session.flush()
        session.clear()

        te = session.retrieve(OrientStringRecordIdEntity, te.id)
        assert te != null
        assert te.id != null
        assert te.name == 'Fred'

        session.delete te
        session.flush()

        te = session.retrieve(OrientStringRecordIdEntity, te.id)
        assert te == null
    }
}

class OrientStringRecordIdEntity {
    String id
    String name
}
