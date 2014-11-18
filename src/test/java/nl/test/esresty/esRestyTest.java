package nl.test.esresty;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.eriky.esResty;

import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;
import us.monoid.web.Resty;

public class esRestyTest {
	esResty r;
	String testIndexName = "esresty-unittest-index-safe-to-delete";
	String testType = "test-type";
	JSONObject testDocument;

	@Before
	public void setUp() throws Exception {
		r = new esResty("http://localhost:9200");
		testDocument = new JSONObject(
				"{ \"name\": \"test\", \"age\": 40, \"post_date\" : \"2009-11-15T14:12:12\" }");
	}

	@After
	public void tearDown() throws Exception {
		// most tests will create the test index, so always try to delete it
		r.deleteIndex(testIndexName);
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testGetStatus() throws IOException, JSONException {
		JSONObject res = r.getBanner();
		assertEquals(res.getInt("status"), 200);
	}

	@Test
	public void testIndexExists() throws JSONException {
		assertFalse(r.indexExists("testeeeenotexistst112234"));
	}

	@Test
	public void testCreateIndex() throws JSONException {
		assertTrue(r.createIndex(testIndexName));
	}

	@Test
	public void testCreateIndexWithSettings() throws JSONException {
		JSONObject indexSettings = new JSONObject(
				"{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0} }");

		assertTrue(r.createIndexWithSettings(testIndexName, indexSettings));
	}

	@Test
	public void testIndex() throws JSONException {
		testCreateIndex();
		assertTrue(r.index(testIndexName, testType, "1", testDocument));
		assertTrue(r.index(testIndexName + "rr", testType, testDocument));
	}

	@Test
	public void testDeleteIndex() throws JSONException {
		testCreateIndex();
		assertTrue(r.deleteIndex(testIndexName));
		assertFalse(r.deleteIndex("doesnotexistsforsureright-11234"));
	}
	
	/*************************************************************************** 
									BULK TESTS
    ****************************************************************************/
	@Test
	public void testBulkIndex() throws JSONException {
		testCreateIndex();
		
		r.setBulkSize(20);
		for (int i=0; i < 20; i++) {
			assertTrue(r.bulkIndex(testIndexName+"rr", testType, Integer.toString(i), testDocument));
		}
		assertEquals(r.getCurrentBulkSize(), 0);
	}

}
