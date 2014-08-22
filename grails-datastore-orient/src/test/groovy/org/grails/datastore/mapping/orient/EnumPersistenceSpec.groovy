package org.grails.datastore.mapping.orient

import org.junit.Test

class EnumPersistenceSpec extends AbstractOrientTest {

    @Test
    void testBasicPersistenceOperations() {
        ds.mappingContext.addPersistentEntity(TestEnumEntity)

        OrientSession session = ds.connect()

        def te = new TestEnumEntity(name:"Bob", type:TestType.T1)

        session.persist te
        session.flush()

        assert te != null
        assert te.id != null
        assert te.id instanceof Long

        session.clear()
        te = session.retrieve(TestEnumEntity, te.id)

        assert te != null
        assert te instanceof TestEnumEntity
        assert te.name == "Bob"
        assert te.type instanceof TestType
        assert te.type == TestType.T1

        te.type = TestType.T2
        session.persist(te)
        session.flush()
        session.clear()

        te = session.retrieve(TestEnumEntity, te.id)
        assert te != null
        assert te.type == TestType.T2
    }
}

class TestEnumEntity {
    Long id
    String name
    TestType type
}

enum TestType {
    T1, T2, T3
}
