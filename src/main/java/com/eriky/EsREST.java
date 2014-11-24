package com.eriky;

import java.io.IOException;

import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;
import us.monoid.web.Resty;
import static us.monoid.web.Resty.content;
import static us.monoid.web.Resty.put;
import static us.monoid.web.Resty.delete;
import static us.monoid.web.Resty.form;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * EsREST class.
 * </p>
 *
 * @author erik
 * @version $Id: $
 */
public class EsREST {
	private Logger log = LoggerFactory.getLogger(EsREST.class);
	private Resty r;
	private String url;
	private JSONObject lastResponse;
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
		r = new Resty();
		url = elasticSearchUrl;
	}

	/**
	 * Get the banner from the root url of ElasticSearch, containing the
	 * tagline, status code and version information.
	 *
	 * @return A JSONObject with the response
	 * @throws java.io.IOException
	 *             if any.
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public JSONObject getBanner() throws IOException, JSONException {
		lastResponse = r.json(url).toObject();
		return lastResponse;
	}

	/**
	 * Retrieve cluster wide health (from hostname:port/_cluster/health).
	 * 
	 * @return A JSONObject with the response
	 * @throws IOException
	 * @throws JSONException
	 */
	public JSONObject getHealth() throws IOException, JSONException {
		String completeUrl = url + "/_cluster/health";
		lastResponse = r.json(completeUrl).toObject();
		return lastResponse;
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
	 * @throws IOException
	 * @throws JSONException
	 */
	public boolean waitForClusterStatus(String status, int timeout)
			throws IOException, JSONException {
		String completeUrl = url + "/_cluster/health?wait_for_status=" + status
				+ "&timeout=" + timeout;
		lastResponse = r.json(completeUrl).toObject();
		return !lastResponse.getBoolean("timed_out");

	}

	/**
	 * Test if index exists by GETting the full url to the index. There is a
	 * better way to do this: by only requesting the HEAD. HEAD requests are not
	 * supported (yet) by Resty.
	 *
	 * @param indexName
	 *            The index name
	 * @return true on success, false otherwise
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean indexExists(String indexName) throws JSONException {
		try {
			lastResponse = r.json(url + '/' + indexName).toObject();
		} catch (IOException e) {
			// Exception is OK, since it simply means the index does not exist
			log.debug("Exception: " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Create index.
	 *
	 * @param indexName
	 *            The name of the to be created index
	 * @return true on success, false otherwise
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean createIndex(String indexName) throws JSONException {
		try {
			lastResponse = r.json(url + '/' + indexName, put(content("")))
					.toObject();
		} catch (IOException e) {
			log.warn("Exception: " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Create index.
	 *
	 * @param indexName
	 *            The index name
	 * @return true on success, false otherwise
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 * @param settings
	 *            a {@link us.monoid.json.JSONObject} object containing the
	 *            index settings.
	 */
	public boolean createIndexWithSettings(String indexName, JSONObject settings)
			throws JSONException {
		try {
			lastResponse = r
					.json(url + '/' + indexName, put(content(settings)))
					.toObject();
		} catch (IOException e) {
			log.warn("Exception: " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Delete index
	 *
	 * @param indexName
	 *            The index name
	 * @return true on success, false otherwise
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean deleteIndex(String indexName) throws JSONException {
		try {
			lastResponse = r.json(url + '/' + indexName, delete()).toObject();
			return true;
		} catch (IOException e) {
			log.warn("Exception: " + e.getMessage());
			return false;
		}
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
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean putMapping(String indexName, String type, JSONObject mapping)
			throws JSONException {

		String completeUrl = url + '/' + indexName + "/_mapping" + '/' + type;

		try {
			lastResponse = r.json(completeUrl, put(content(mapping)))
					.toObject();
		} catch (IOException e) {
			log.warn("Exception: " + e.getMessage());
			return false;
		}
		return true;
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
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean index(String indexName, String type, String id,
			JSONObject document) throws JSONException {

		String completeUrl = url + '/' + indexName + '/' + type + '/' + id;

		try {
			lastResponse = r.json(completeUrl, put(content(document)))
					.toObject();
			return true;
		} catch (IOException e) {
			log.warn("Exception: " + e.getMessage());
			return false;
		}
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
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean index(String indexName, String type, JSONObject document)
			throws JSONException {

		String completeUrl = url + '/' + indexName + '/' + type + '/';

		try {
			lastResponse = r.json(completeUrl, form(document.toString()))
					.toObject();
			return true;
		} catch (IOException e) {
			log.warn("Exception: " + e.getMessage());
			return false;
		}
	}

	/**
	 * Get the last response from ElasticSearch as JSONObject. Note that if the
	 * last request failed due to a network error, this will return the last
	 * response before the network problem started.
	 *
	 * @return a JSONObject with the response to the last request that was made.
	 */
	public JSONObject getLastResponse() {
		return lastResponse;
	}

	/**
	 * Set the maximum size of the bulk queue.
	 *
	 * @param numberOfDocuments
	 *            a int.
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
	 *            a {@link us.monoid.json.JSONObject} object.
	 * @return a boolean.
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean bulkIndex(String indexName, String type, String id,
			JSONObject document) throws JSONException {
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

	private void addIndexActionToBulk(String indexName, String type, String id,
			JSONObject document) {
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

	/**
	 * <p>
	 * doBulkRequest
	 * </p>
	 *
	 * @return a boolean.
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	public boolean doBulkRequest() throws JSONException {
		try {
			String bulkRequest = bulkStringBuffer.toString();
			log.debug(bulkRequest);
			lastResponse = r.json(url + "/_bulk", put(content(bulkRequest)))
					.toObject();
			return true;
		} catch (IOException e) {
			log.error("Exception: " + e.getMessage());
			e.printStackTrace();
			log.error(lastResponse.toString(2));
			return false;
		}

	}
}
