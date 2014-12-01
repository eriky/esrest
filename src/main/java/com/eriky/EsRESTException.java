package com.eriky;

public class EsRESTException extends Exception {
    public EsRESTException() {
        super();
    }
    
    public EsRESTException(String message) {
        super(message);
    }
    
    public EsRESTException(Exception e) {
        super(e);
    }
}
