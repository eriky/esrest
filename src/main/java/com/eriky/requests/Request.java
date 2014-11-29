package com.eriky.requests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;

public abstract class Request {
    protected Logger log = LoggerFactory.getLogger(this.getClass());
    
    protected boolean compareResponseCode(HttpRequest result, int expectedCode) {
        try {
            if (result.asString().getStatus() == expectedCode) {
                return true;
            } else {
                return false;
            }
        } catch (UnirestException e) {
            log.error(e.getMessage());
            return false;
        }
    }
}
