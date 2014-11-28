package com.eriky;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import com.mashape.unirest.request.HttpRequest;
import com.mashape.unirest.request.HttpRequestWithBody;

/**
 * <p>
 * EsREST class.
 * </p>
 *
 * @author eriky
 * @version $Id: $
 */
public class EsREST {
	private Logger log = LoggerFactory.getLogger(EsREST.class);
	private String url;
	private int bulkSize = 200;
	private int currentBulkSize = 0;
	private StringBuffer bulkStringBuffer = new StringBuffer();

	/**
	 * Create a new esResty client.
	 *
	 * @param elasticSearchUrl
	 *            the full url to the ElasticSearch server without trailing
	 *            slash, e.g. http://localhost:9200
	 */
	public EsREST(String elasticSearchUrl) {
		url = elasticSearchUrl;
	}

	/**
	 * Get the banner from the root url of ElasticSearch, containing the
	 * tagline, status code and version information.
	 *
	 * @return A JSONObject with the response
	 * @throws UnirestException
	 */
	public org.json.JSONObject getBanner() throws UnirestException {
		return Unirest.get(url).asJson().getBody().getObject();
	}

	/**
	 * Retrieve cluster wide health (from hostname:port/_cluster/health).
	 * 
	 * @return A JSONObject with the response
	 * @throws UnirestException
	 */
	public org.json.JSONObject getHealth() throws UnirestException {
		String completeUrl = url + "/_cluster/health";
		return Unirest.get(completeUrl).asJson().getBody().getObject();

	}

	/**
	 * Will wait (until the timeout as provided) until the status of the cluster
	 * changes to the one provided or better, i.e. green > yellow > red.
	 * 
	 * @param status
	 *            One of green, yellow or red.
	 * 
	 * @param timeout
	 *            How long to wait, in seconds.
	 * @return true if status was reached, false if status is not reached
	 * @throws UnirestException
	 */
	public boolean waitForClusterStatus(String status, int timeout)
			throws UnirestException {
		org.json.JSONObject json;
		HttpResponse<JsonNode> result = Unirest.get(
				url + "/_cluster/health?wait_for_status=" + status
						+ "&timeout=" + timeout + "s").asJson();

		json = result.getBody().getObject();

		if (result.getStatus() == 200) {
			return !json.getBoolean("timed_out");
		} else {
			return false;
		}
	}

	/**
	 * Check if index exists.
	 *
	 * @param indexName
	 *            The index name
	 * @return true if index exists, false otherwise
	 */
	public boolean indexExists(String indexName) {
		GetRequest result = Unirest.head(url + '/' + indexName);
		return compareResponseCode(result, 200);
	}

	/**
	 * Create index.
	 *
	 * @param indexName
	 *            The name of the to be created index
	 * @return true on success, false otherwise
	 */
	public boolean createIndex(String indexName) {
		HttpRequestWithBody result = Unirest.put(url + "/{index}").routeParam(
				"index", indexName);

		return compareResponseCode(result, 200);
	}

	/**
	 * Create index.
	 *
	 * @param indexName
	 *            The index name
	 * @return true on success, false otherwise
	 * @param settings
	 *            a {@link org.json.JSONObject} object containing the index
	 *            settings.
	 */
	public boolean createIndexWithSettings(String indexName,
			org.json.JSONObject settings) {

		HttpRequest result = Unirest.put(url + '/' + indexName)
				.body(settings.toString()).getHttpRequest();
		return compareResponseCode(result, 200);
	}

	/**
	 * Delete index
	 *
	 * @param indexName
	 *            The index name
	 * @return true on success, false otherwise
	 */
	public boolean deleteIndex(String indexName) {
		HttpRequest result = Unirest.delete(url + '/' + indexName)
				.getHttpRequest();
		return compareResponseCode(result, 200);
	}

	/**
	 * Put mapping for index and type.
	 *
	 * @param indexName
	 *            the index name
	 * @param type
	 *            the document type
	 * @param mapping
	 *            a {@link us.monoid.json.JSONObject} object containing the
	 *            mapping.
	 * @return true on success, false otherwise
	 */
	public boolean putMapping(String indexName, String type,
			org.json.JSONObject mapping) {

		String completeUrl = url + '/' + indexName + '/' + type + "/_mapping";
		// Since Elasticsearch 1.x, this should be:
		// String completeUrl = url + '/' + indexName + "/_mapping" + '/' +
		// type;
		// But up to 1.4.0, ES is backwards compatible with the old url so we
		// keep it this way for now
		HttpRequest result = Unirest.put(completeUrl).body(mapping.toString())
				.getHttpRequest();
		return compareResponseCode(result, 200);
	}

	/**
	 * Create an index alias.
	 * 
	 * @param indexName
	 *            the index name you want to add an alias to
	 * @param alias
	 *            the alias name
	 * @return true on success, false otherwise
	 */
	public boolean createAlias(String indexName, String alias) {
		String completeUrl = url + '/' + indexName + "/_alias" + '/' + alias;
		HttpRequest result = Unirest.put(completeUrl).getHttpRequest();
		return compareResponseCode(result, 200);
	}

	/**
	 * Create an index alias with filter and optional routing. The filter and
	 * routing must be created as explained in the ElasticSearch documentation.
	 * This method will not check for validity.
	 * 
	 * @param indexName
	 *            the index name you want to add an alias to
	 * @param alias
	 *            the alias name
	 * @param filter
	 *            a JSONDocument containing a valid alias filter and optional
	 *            routing
	 * @return true on success, false otherwise
	 */
	public boolean createFilterAlias(String indexName, String alias,
			org.json.JSONObject filter) {
		String completeUrl = url + '/' + indexName + "/_alias" + '/' + alias;

		HttpRequest result = Unirest.put(completeUrl).body(filter.toString())
				.getHttpRequest();
		return compareResponseCode(result, 200);
	}

	/**
	 * Index the given document with the given id into given index and type.
	 *
	 * @param indexName
	 *            the index name
	 * @param type
	 *            the document type
	 * @param id
	 *            the id of the document
	 * @param document
	 *            The document as a {@link us.monoid.json.JSONObject} object
	 *            containing the mapping.
	 * @return true on success, false otherwise
	 */
	public boolean index(String indexName, String type, String id,
			org.json.JSONObject document) {

		String completeUrl = url + '/' + indexName + '/' + type + '/' + id;

		HttpRequest result = Unirest.put(completeUrl).body(document.toString())
				.getHttpRequest();
		return compareResponseCode(result, 201);
	}

	/**
	 * Index the given document into given index and type. An id will be
	 * automatically created by ElasticSearch.
	 *
	 * @param indexName
	 *            the index name
	 * @param type
	 *            the document type
	 * @param document
	 *            the document as a JSONObject
	 * @return true on success, false otherwise
	 */
	public boolean index(String indexName, String type,
			org.json.JSONObject document) {
		String completeUrl = url + '/' + indexName + '/' + type;
		HttpRequest result = Unirest.post(completeUrl)
				.body(document.toString()).getHttpRequest();
		return compareResponseCode(result, 201);
	}

	/**
	 * Set the maximum size of the bulk queue.
	 *
	 * @param numberOfDocuments
	 *            the number of docs after which the queue will be submitted to
	 *            ES.
	 */
	public void setBulkSize(int numberOfDocuments) {
		bulkSize = numberOfDocuments;
	}

	/**
	 * Get the number of documents that are currently in the queue for the next
	 * bulk request.
	 *
	 * @return the number of queued documents
	 */
	public int getCurrentBulkSize() {
		return currentBulkSize;
	}

	/**
	 * <p>
	 * bulkIndex
	 * </p>
	 *
	 * @param indexName
	 *            a {@link java.lang.String} object.
	 * @param type
	 *            a {@link java.lang.String} object.
	 * @param id
	 *            a {@link java.lang.String} object.
	 * @param document
	 *            a {@link org.json.JSONObject} object.
	 * @return a boolean.
	 */
	public boolean bulkIndex(String indexName, String type, String id,
			org.json.JSONObject document) {
		addIndexActionToBulk(indexName, type, id, document);

		if (currentBulkSize == bulkSize) {
			boolean success = doBulkRequest();
			if (success) {
				currentBulkSize = 0;
				bulkStringBuffer.setLength(0);
			}
			return success;
		} else {
			return true;
		}
	}

	/**
	 * <p>
	 * doBulkRequest
	 * </p>
	 *
	 * @return a boolean.
	 */
	public boolean doBulkRequest() {

		String bulkRequest = bulkStringBuffer.toString();
		String completeUrl = url + "/_bulk";

		HttpRequest result = Unirest.put(completeUrl).body(bulkRequest)
				.getHttpRequest();
		try {
			if (result.asJson().getBody().getObject().getBoolean("errors")) {
				return false;
			}
		} catch (org.json.JSONException e) {
			log.warn("Could not check for errors in JSON, most probably you are using an old version of Elasticsearch.");
			log.warn("The errors field in bulk responses was added in ES 1.0. Exception was:");
			log.warn(e.getMessage());
			return true;
		} catch (UnirestException e) {
			log.error(e.getMessage());
			return false;
		}

		return compareResponseCode(result, 200);
	}

	private void addIndexActionToBulk(String indexName, String type, String id,
			org.json.JSONObject document) {
		StringBuffer sb = new StringBuffer();
		sb.append("{ \"index\" : { \"_index\" : \"");
		sb.append(indexName);
		sb.append("\", \"_type\" : \"");
		sb.append(type);
		sb.append("\", \"_id\" : \"");
		sb.append(id);
		sb.append("\" } }");
		String action = sb.toString();
		String documentJson = document.toString();
		bulkStringBuffer.append(action);
		bulkStringBuffer.append('\n');
		bulkStringBuffer.append(documentJson);
		bulkStringBuffer.append('\n');
		currentBulkSize += 1;
	}

	private boolean compareResponseCode(HttpRequest result, int expectedCode) {
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
