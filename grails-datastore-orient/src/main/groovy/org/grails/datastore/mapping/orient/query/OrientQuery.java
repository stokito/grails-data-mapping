package org.grails.datastore.mapping.orient.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.grails.datastore.mapping.model.PersistentEntity;
import org.grails.datastore.mapping.orient.OrientSession;
import org.grails.datastore.mapping.query.Query;
import org.grails.datastore.mapping.query.Restrictions;
import org.grails.datastore.mapping.query.api.QueryArgumentsAware;
import org.grails.datastore.mapping.query.jpa.JpaQueryBuilder;
import org.grails.datastore.mapping.query.jpa.JpaQueryInfo;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;
import java.util.List;
import java.util.Map;

public class OrientQuery extends Query implements QueryArgumentsAware {
    private static final Log LOG = LogFactory.getLog(OrientQuery.class);

    public OrientQuery(OrientSession session, PersistentEntity entity) {
        super(session, entity);

        if (session == null) {
            throw new InvalidDataAccessApiUsageException("Argument session cannot be null");
        }
        if (entity == null) {
            throw new InvalidDataAccessApiUsageException("No persistent entity specified");
        }
    }

    @Override
    public OrientSession getSession() {
        return (OrientSession) super.getSession();
    }

    @Override
    public void add(Criterion criterion) {
        if (criterion instanceof Equals) {
            final Equals eq = (Equals) criterion;
            Object resolved = resolveIdIfEntity(eq.getValue());
            if (resolved != eq.getValue()) {
                criterion = Restrictions.idEq(resolved);
            }
        }

        criteria.add(criterion);
    }

    @Override
    protected List executeQuery(final PersistentEntity entity, final Junction criteria) {
        /*
        final JpaTemplate jpaTemplate = getSession().getJpaTemplate();
        if (!OrientSession.hasTransaction()) {
            jpaTemplate.setFlushEager(false);
        }

        return (List)jpaTemplate.execute(new JpaCallback<Object>() {
            public Object doInJpa(EntityManager em) throws PersistenceException {
                return executeQuery(entity, criteria, em, false);
            }
        });
        */
        return null;
    }

    @Override
    public Object singleResult() {
        /*
        final JpaTemplate jpaTemplate = getSession().getJpaTemplate();
        if (!JpaSession.hasTransaction()) {
            jpaTemplate.setFlushEager(false);
        }
        try {
            return jpaTemplate.execute(new JpaCallback<Object>() {
                public Object doInJpa(EntityManager em) throws PersistenceException {
                    return executeQuery(entity, criteria, em, true);
                }
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
        */
        return null;

    }

    Object executeQuery(final PersistentEntity entity, final Junction criteria, EntityManager em, boolean singleResult) {

        JpaQueryBuilder queryBuilder = new JpaQueryBuilder(entity, criteria, projections, orderBy);
        queryBuilder.setConversionService(session.getDatastore().getMappingContext().getConversionService());
        JpaQueryInfo jpaQueryInfo = queryBuilder.buildSelect();
        List parameters = jpaQueryInfo.getParameters();
        final String queryToString = jpaQueryInfo.getQuery();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Built JPQL to execute: " + queryToString);
        }
        final javax.persistence.Query q = em.createQuery(queryToString);

        if (parameters != null) {
            for (int i = 0, count = parameters.size(); i < count; i++) {
                q.setParameter(i + 1, parameters.get(i));
            }
        }
        q.setFirstResult(offset);
        if (max > -1) {
            q.setMaxResults(max);
        }

        if (!singleResult) {
            return q.getResultList();
        }
        return q.getSingleResult();
    }

    @Override
    public void setArguments(@SuppressWarnings("rawtypes") Map arguments) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
