package com.eriky.requests;

import org.json.JSONObject;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;

public class Get extends Request {
    private String routing = null;
    private String fields = null;
    private boolean includeSource = true;
    private boolean sourceOnly = false;
    
    private String url;
    private String indexName;
    private String type;
    private String id;
    
    public Get(String url, String indexName, String type, String id) {
        this.url = url;
        this.indexName = indexName;
        this.type = type;
        this.id = id;
    }
    
    public Get routing(String routing) {
        this.routing = routing;
        return this;
    }
    
    public Get source(boolean includeSource) {
        this.includeSource = includeSource;
        return this;
    }
    
    public Get sourceOnly(boolean sourceOnly) {
        this.sourceOnly = sourceOnly;
        return this;
    }
    
    public Get fields(String fields) {
        this.fields = fields;
        return this;
    }
    
    public JSONObject execute() {
        String completeUrl = url + '/' + indexName + '/' + type + '/' + id;
        if (sourceOnly) {
            completeUrl += "/_source";
        }
        HttpRequest httpRequest = Unirest.get(completeUrl);
        
        httpRequest.queryString("source", includeSource);
        if (routing != null) {
            httpRequest.queryString("routing", routing);
        }
        
        if (fields != null) {
            httpRequest.queryString("fields", fields);
        }
        log.info("all parameters set for request: " + httpRequest.getUrl());
        if (compareResponseCode(httpRequest, 200)) {
            try {
                return httpRequest.asJson().getBody().getObject();
            } catch (UnirestException e) {
                log.error(e.getMessage());
            }
        } else {
            log.warn("Failed to GET from "
                    + completeUrl
                    + " : Elasticsearch returned a status code that was not 200.");
        }
        return null;
    }
}
