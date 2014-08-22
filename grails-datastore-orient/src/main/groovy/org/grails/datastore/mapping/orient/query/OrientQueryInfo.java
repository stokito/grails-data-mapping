package org.grails.datastore.mapping.orient.query;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: emrul
 * Date: 13/08/13
 * Time: 23:21
 * To change this template use File | Settings | File Templates.
 */
public class OrientQueryInfo {
    String query;
    List parameters;

    public OrientQueryInfo(String query, List parameters) {
        this.query = query;
        this.parameters = parameters;
    }

    public String getQuery() {
        return query;
    }

    public List getParameters() {
        return parameters;
    }
}
