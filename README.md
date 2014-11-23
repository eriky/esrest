# EsREST
Welcome to EsREST. This is a simple, straightforward Java client to the ElasticSearch REST API.

## About this little project
If you found this project, you probably know Jest: the only Java based REST client for ElasticSearch.
I was not satisfied with the way Jest works. Some of my frustrations:

* jest still has a requirement on ElasticSearch if you want to create a mapping with settings
* most of the times it is really hard / impossible to check if an operation succeeded
* documentation for Jest is lacking. There are samples but there is no API documentation

EsREST does not depend on the ElasticSearch jar. EsREST will give you back information on wether a request
succeeded or failed. But please keep reading, since EsREST is far from perfect too.

## Resty
I used another *not so complete, not completely finished* Java project called Resty as a base. Resty handles
all the actual REST requests and does so in an elegant manner.

## Want to use this project?
Feel free to use EsREST. There are no guarantees though.
EsREST is by no means a complete or even half decent alternative to Jest or the official client.
I basically implemented what *I* needed, and that's it. However I welcome you to try EsREST and add 
(or request) more functionality.

## How to use EsREST
Since you are a coder, for now I would like to refer you to the unit tests of EsREST. I try to cover as
much code as possible with the unit tests, so there is example code of all the public EsREST methods.

## TODO's
* Write documentation with samples
* Build API documentation
