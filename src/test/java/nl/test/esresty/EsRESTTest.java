package nl.test.esresty;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.eriky.EsREST;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * <p>
 * EsRESTTest class.
 * </p>
 *
 * @author erik
 * @version $Id: $
 * @since 0.1.1
 */
public class EsRESTTest {
	EsREST r;
	String testIndexName = "esresty-unittest-index-safe-to-delete";
	String testType = "test-type";
	String testAliasName = "esresty-unittest-index-safe-to-delete-alias";
	org.json.JSONObject testDocument;
	org.json.JSONObject testMapping;
	
	String mappingString = "{ \""
			+ this.testType
			+ "\": { \"properties\": { \"age\": { \"type\": \"string\" } }}}";

	/**
	 * <p>
	 * setUp
	 * </p>
	 *
	 * @throws java.lang.Exception
	 *             if any.
	 */
	@Before
	public void setUp() throws Exception {
		r = new EsREST("http://localhost:9200");
//		if (!r.waitForClusterStatus("yellow", 2)) {
//			System.err
//					.println("ERROR: Elasticsearch cluster status should be at least yellow to perform these unit tests");
//		}
		testMapping = new org.json.JSONObject(mappingString);
		testDocument = new org.json.JSONObject(
				"{ \"name\": \"test\", \"age\": 40, \"post_date\" : \"2009-11-15T14:12:12\" }");
	}

	/**
	 * <p>
	 * tearDown
	 * </p>
	 *
	 * @throws java.lang.Exception
	 *             if any.
	 */
	@After
	public void tearDown() throws Exception {
		// most tests will create the test index, so always try to delete it
		r.deleteIndex(testIndexName);
	}

	/**
	 * <p>
	 * setUpBeforeClass
	 * </p>
	 *
	 * @throws java.lang.Exception
	 *             if any.
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * <p>
	 * tearDownAfterClass
	 * </p>
	 *
	 * @throws java.lang.Exception
	 *             if any.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * <p>
	 * testGetStatus
	 * </p>
	 *
	 * @throws java.io.IOException
	 *             if any.
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 * @throws UnirestException 
	 */
	@Test
	public void testGetStatus() throws UnirestException {
		org.json.JSONObject res = r.getBanner();
		assertEquals(res.getInt("status"), 200);
	}

	@Test
	public void testGetHealth() throws UnirestException {
		org.json.JSONObject res = r.getHealth();
		assertEquals(res.getInt("number_of_nodes"), 1);
	}

	@Test
	public void testWaitForYellowStatus() throws UnirestException {
		boolean res = r.waitForClusterStatus("yellow", 2);
		assertTrue(res);
	}
	
	@Test
    public void testWaitForGreenStatus() throws UnirestException {
	    // create an index, by default with replica's so status will be yellow
	    r.createIndex(testIndexName);
        boolean res = r.waitForClusterStatus("green", 1);
        assertFalse(res);
    }

	@Test
	public void testCreateAlias() {
		r.createIndex(testIndexName);
		boolean res = r.createAlias(testIndexName, testAliasName);
		assertTrue(res);
	}

	@Test
	public void testCreateFilterAlias() {
		// We need an index and a document with the field "age" before being
		// able to create a filtered alias on that field
		r.createIndex(testIndexName);
		r.index(testIndexName, testType, testDocument);
		org.json.JSONObject filter = new org.json.JSONObject(
				"{\"filter\" : { \"term\" : { \"age\" : 40 } } }");
		boolean res = r.createFilterAlias(testIndexName, testAliasName, filter);
		assertTrue(res);
	}

	/**
	 * <p>
	 * testIndexExists
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testIndexExists() {
		r.createIndex(testIndexName);
		//assertFalse(r.indexExists("testeeeenotexistst112234"));
		assertTrue(r.indexExists(testIndexName));
	}

	/**
	 * <p>
	 * testCreateIndex
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testCreateIndex() {
		assertTrue(r.createIndex(testIndexName));
	}

	/**
	 * <p>
	 * testCreateIndexWithSettings
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testCreateIndexWithSettings() {
		org.json.JSONObject indexSettings = new org.json.JSONObject(
				"{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0} }");

		assertTrue(r.createIndexWithSettings(testIndexName, indexSettings));
	}

	/**
	 * <p>
	 * testIndexDocWithId
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testIndexDocWithId() {
		r.createIndex(testIndexName);
		assertTrue(r.index(testIndexName, testType, "1", testDocument));
	}

	/**
	 * <p>
	 * testIndexDocWithoutId
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testIndexDocWithoutId() {
		r.createIndex(testIndexName);
		assertTrue(r.index(testIndexName, testType, testDocument));
	}

	/**
	 * <p>
	 * testDeleteIndexThatExists
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testDeleteIndexThatExists() {
		testCreateIndex();
		assertTrue(r.deleteIndex(testIndexName));
	}

	/**
	 * <p>
	 * testDeleteIndexThatDoesNotExist
	 * </p>
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testDeleteIndexThatDoesNotExist() {
		testCreateIndex();
		assertFalse(r.deleteIndex("doesnotexistsforsureright-11234"));
	}

	@Test
	public void testPutMapping() {
		testCreateIndex();
		assertTrue(r.putMapping(testIndexName, testType, testMapping));
	}

	/**
	 *************************************************************************
	 * BULK TESTS
	 ***************************************************************************
	 *
	 * @throws us.monoid.json.JSONException
	 *             if any.
	 */
	@Test
	public void testValidBulkIndex() {
		testCreateIndex();

		r.setBulkSize(20);
		for (int i = 0; i < 20; i++) {
			assertTrue(r.bulkIndex(testIndexName, testType,
					Integer.toString(i), testDocument));
		}
		assertEquals(r.getCurrentBulkSize(), 0);
	}
}
