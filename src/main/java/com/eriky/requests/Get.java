package com.eriky.requests;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.eriky.EsRESTException;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;

public class Get extends Request {
    private boolean sourceOnly = false;

    private String url;
    private String indexName;
    private String type;
    private String id;
    private Map<String, Object> queryStrings = new HashMap<String, Object>();

    public Get(String url) {
        this.url = url;
        type = "_all";
    }

    public Get withUrl(String url) {
        this.url = url;
        return this;
    }

    public Get withIndex(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public Get withType(String type) {
        this.type = type;
        return this;
    }

    public Get id(String id) {
        this.id = id;
        return this;
    }

    public Get sourceOnly(boolean sourceOnly) {
        this.sourceOnly = sourceOnly;
        return this;
    }

    /* All options that are passed to ES by query string go here */
    public Get routing(String routing) {
        queryStrings.put("routing", routing);
        return this;
    }

    public Get includeSource(boolean includeSource) {
        queryStrings.put("source", includeSource);
        return this;
    }

    public Get fields(String fields) {
        queryStrings.put("fields", fields);
        return this;
    }

    /**
     * See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html
     * 
     * @param preference the preference String
     * @return
     */
    public Get preference(String preference) {
        queryStrings.put("preference", preference);
        return this;
    }

    public Get refresh(boolean refresh) {
        queryStrings.put("refresh", refresh);
        return this;
    }

    public Get version(String version) {
        queryStrings.put("version", version);
        return this;
    }

    public JSONObject execute() throws EsRESTException {
        if (indexName == null) {
            throw new EsRESTException("No index name specified");
        }

        if (id == null) {
            throw new EsRESTException("No document id specified");
        }

        String completeUrl = getUrl();

        HttpRequest httpRequest = Unirest.get(completeUrl).queryString(
                queryStrings);

        log.debug("all parameters set for request: " + httpRequest.getUrl());

        if (compareResponseCode(httpRequest, 200)) {
            try {
                return httpRequest.asJson().getBody().getObject();
            } catch (UnirestException e) {
                log.error(e.getMessage());
                throw new EsRESTException(e);
            }
        } else {
            log.warn("Expected 200 OK from a GET to " + httpRequest.getUrl()
                    + " but got another status code instead");
        }
        return null;
    }

    public String getUrl() {
        String completeUrl = url + '/' + indexName + '/' + type + '/' + id;
        if (sourceOnly) {
            completeUrl += "/_source";
        }
        return completeUrl;
    }

    public String getIndex() {
        return indexName;
    }

    public String getType() {
        return type;
    }

    public Object getQueryString(String key) {
        return this.queryStrings.get(key);
    }
}
