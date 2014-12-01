# EsREST
Welcome to EsREST. This is a simple, straightforward Java client to the Elasticsearch REST API.

[![Build Status](https://travis-ci.org/eriky/esrest.svg)](https://travis-ci.org/eriky/esrest)
[![Coverage Status](https://coveralls.io/repos/eriky/esrest/badge.png?branch=master)](https://coveralls.io/r/eriky/esrest?branch=master)

## About this little project
If you found this project, you probably know Jest: the only Java based REST client for Elasticsearch.
I was not satisfied with the way Jest works. 

EsREST has a number of advantages:

* The EsREST API is very intuitive (see examples below) because of method chaining
* EsREST does not depend on the Elasticsearch jar
* EsREST will clearly indicate wether a request succeeded or failed

There are some disavantages too though:

* This project is very new and under active development
* Only the most basic Elasticsearch API calls are implement right now

## Usage examples
Here are some basic usage examples:

    EsREST e = new EsREST("http://localhost:9200");
    e.createIndex("my-index");
    JSONDocument doc = e.getDocument().id("abc").fromIndex("my-index").ofType("my-type");
    System.out.println(doc.getString["id"]);

## Want to use this project?
Feel free to use EsREST. There are no guarantees though.
EsREST is by no means a complete or even half decent alternative to Jest or the official client.
I basically implemented what *I* needed, and that's it. However I welcome you to try EsREST and add 
(or request) more functionality.

## How to use EsREST
For now I would like to refer you to the unit tests of EsREST. I try to cover as
much code as possible with the unit tests, so there should be example code for all the public EsREST methods.

## TODO's
* Continue rewrite of fluent API style
* Improve bulk requests / do extensive testing to prevent OOME
* Throw our own EsRESTException instead of exposing Unirest
* Write documentation with samples
* Build API documentation
