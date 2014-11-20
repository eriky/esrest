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

public class esResty {
	private Logger log = LoggerFactory.getLogger(esResty.class);
	private Resty r;
	private String url;
	private JSONObject lastResponse;
	private int bulkSize = 200;
	private int currentBulkSize = 0;
	private StringBuffer bulkStringBuffer = new StringBuffer();
	
	public esResty(String elasticSearchUrl) {
		r = new Resty();
		url = elasticSearchUrl;
	}

	/**
	 * Get the banner from the root url of ElasticSearch, containing the
	 * tagline, status code and version information.
	 * 
	 * @return A JSONObject with the response
	 * 
	 * @throws IOException
	 * @throws JSONException
	 */
	public JSONObject getBanner() throws IOException, JSONException {
		lastResponse = r.json(url).toObject();
		return lastResponse;
	}

	/**
	 * Test if index exists by GETting the full url to the index. There is a
	 * better way to do this: by only requesting the HEAD. HEAD requests are not
	 * supported (yet) by Resty.
	 * 
	 * @param indexName
	 * @return true on success, false otherwise
	 * @throws JSONException
	 */
	public boolean indexExists(String indexName) throws JSONException {
		try {
			lastResponse = r.json(url + '/' + indexName).toObject();
		} catch (IOException e) {
		    log.error("Exception: " + e .getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Create index.
	 * 
	 * @param indexName
	 * @return true on success, false otherwise
	 * @throws JSONException
	 */
	public boolean createIndex(String indexName) throws JSONException {
		try {
			lastResponse = r.json(url + '/' + indexName, put(content("")))
					.toObject();
		} catch (IOException e) {
		    log.error("Exception: " + e .getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Create index.
	 * 
	 * @param indexName
	 * @return true on success, false otherwise
	 * @throws JSONException
	 */
	public boolean createIndexWithSettings(String indexName, JSONObject settings)
			throws JSONException {
		try {
			lastResponse = r
					.json(url + '/' + indexName, put(content(settings)))
					.toObject();
		} catch (IOException e) {
		    log.error("Exception: " + e .getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Delete index
	 * 
	 * @param indexName
	 * @return true on success, false otherwise
	 * @throws JSONException
	 */
	public boolean deleteIndex(String indexName) throws JSONException {
		try {
			lastResponse = r.json(url + '/' + indexName, delete()).toObject();
			return true;
		} catch (IOException e) {
		    log.error("Exception: " + e .getMessage());
			return false;
		}
	}

	/**
	 * Index the given document with the given id into given index and type.
	 * 
	 * @param indexName
	 * @param type
	 * @param id
	 * @param document
	 * @return
	 * @throws JSONException
	 */
	public boolean index(String indexName, String type, String id,
			JSONObject document) throws JSONException {

		String completeUrl = url + '/' + indexName + '/' + type + '/' + id;

		try {
			lastResponse = r.json(completeUrl, put(content(document)))
					.toObject();
			return true;
		} catch (IOException e) {
		    log.error("Exception: " + e .getMessage());
			return false;
		}
	}

	/**
	 * Index the given document into given index and type. An id will be
	 * automatically created by ElasticSearch.
	 * 
	 * @param indexName
	 * @param type
	 * @param id
	 * @param document
	 * @return
	 * @throws JSONException
	 */
	public boolean index(String indexName, String type, JSONObject document)
			throws JSONException {

		String completeUrl = url + '/' + indexName + '/' + type + '/';

		try {
			lastResponse = r.json(completeUrl, form(document.toString()))
					.toObject();
			return true;
		} catch (IOException e) {
		    log.error("Exception: " + e .getMessage());
			return false;
		}
	}

	/**
	 * Get the as JSONObject formatted response to the last request that was
	 * made.
	 * 
	 * @return
	 */
	public JSONObject getLastResponse() {
		return lastResponse;
	}

	/** 
	 * Set the maximum size of the bulk queue.
	 * 
	 * @param numberOfDocuments
	 */
	public void setBulkSize(int numberOfDocuments) {
		bulkSize = numberOfDocuments;
	}

	/**
	 * Get the number of documents that are currently in the queue for the next
	 * bulk request.
	 * 
	 * @return
	 */
	public int getCurrentBulkSize() {
		return currentBulkSize;
	}

	public boolean bulkIndex(String indexName, String type,
			String id, JSONObject document) throws JSONException {
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
	
	
	public boolean doBulkRequest() throws JSONException {
		try {
			String bulkRequest = bulkStringBuffer.toString();
			log.debug(bulkRequest);
			lastResponse = r.json(url + "/_bulk", put(content(bulkRequest)))
					.toObject();
			return true;
		} catch (IOException e) {
			log.error("Exception: " + e .getMessage());
			e.printStackTrace();
			log.error(lastResponse.toString(2));
			return false;
		}
		
	}
}
