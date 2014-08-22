package org.grails.datastore.mapping.orient

import com.orientechnologies.orient.core.id.ORecordId
import org.junit.Test

class BasicPersistenceSpec extends AbstractOrientTest {

    @Test
    void testBasicPersistenceOperations() {
        ds.mappingContext.addPersistentEntity(TestEntity)

        OrientSession session = ds.connect()

        def te = new TestEntity(name:"Bob")

        session.persist te
        session.flush()

        assert te != null
        assert te.id != null
        assert te.id instanceof ORecordId

        session.clear()
        te = session.retrieve(TestEntity, te.id)

        assert te != null
        assert te.name == "Bob"

        te.name = "Fred"
        session.persist(te)
        session.flush()
        session.clear()

        te = session.retrieve(TestEntity, te.id)
        assert te != null
        assert te.id != null
        assert te.name == 'Fred'

        session.delete te
        session.flush()

        te = session.retrieve(TestEntity, te.id)
        assert te == null
    }
}

class TestEntity {
    ORecordId id
    String name
}
