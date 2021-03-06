package nl.test.esresty;

import static org.junit.Assert.*;

import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.eriky.EsREST;
import com.eriky.EsRESTException;
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
    EsREST rValid, rInvalid;
    String testIndexName = "esresty-unittest-index-safe-to-delete";
    String testType = "test-type";
    String testAliasName = "esresty-unittest-index-safe-to-delete-alias";
    org.json.JSONObject testDocument;
    org.json.JSONObject testMapping;

    String mappingString = "{ \"" + this.testType
            + "\": { \"properties\": { \"age\": { \"type\": \"integer\" } }}}";

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
        rValid = new EsREST("http://localhost:9200");
        rInvalid = new EsREST("http://localhost:9201");
        // if (!r.waitForClusterStatus("yellow", 2)) {
        // System.err
        // .println("ERROR: Elasticsearch cluster status should be at least yellow to perform these unit tests");
        // }
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
        rValid.deleteIndex(testIndexName);
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
     * @throws UnirestException
     */
    @Test
    public void testGetStatus() throws UnirestException {
        org.json.JSONObject res = rValid.getBanner();
        assertEquals(res.getInt("status"), 200);
    }

    @Test
    public void testGetHealth() throws UnirestException {
        org.json.JSONObject res = rValid.getHealth();
        assertEquals(res.getInt("number_of_nodes"), 1);
    }

    @Test
    public void testWaitForYellowStatus() throws UnirestException {
        assertTrue(rValid.waitForClusterStatus("yellow", 1));
    }

    @Test
    public void testWaitForGreenStatus() throws UnirestException {
        // create an index, by default with replica's so status will be yellow
        rValid.createIndex(testIndexName);
        assertFalse(rValid.waitForClusterStatus("green", 1));
    }

    @Test
    public void testCreateAlias() {
        rValid.createIndex(testIndexName);
        assertTrue(rValid.createAlias(testIndexName, testAliasName));
    }

    @Test
    public void testCreateAliasInvalidUrl() {
        assertFalse(rInvalid.createAlias(testIndexName, testAliasName));
    }

    @Test
    public void testCreateFilterAlias() {
        // We need an index and a document with the field "age" before being
        // able to create a filtered alias on that field
        rValid.createIndex(testIndexName);
        rValid.index(testIndexName, testType, testDocument);
        org.json.JSONObject filter = new org.json.JSONObject(
                "{\"filter\" : { \"term\" : { \"age\" : 40 } } }");
        boolean res = rValid.createFilterAlias(testIndexName, testAliasName,
                filter);
        assertTrue(res);
    }

    /**
     * <p>
     * testIndexExists
     * </p>
     *
     */
    @Test
    public void testIndexExists() {
        rValid.createIndex(testIndexName);
        // assertFalse(r.indexExists("testeeeenotexistst112234"));
        assertTrue(rValid.indexExists(testIndexName));
    }

    /**
     * <p>
     * testCreateIndex
     * </p>
     *
     */
    @Test
    public void testCreateIndex() {
        assertTrue(rValid.createIndex(testIndexName));
    }

    /**
     * <p>
     * testCreateIndexWithSettings
     * </p>
     *
     */
    @Test
    public void testCreateIndexWithSettings() {
        org.json.JSONObject indexSettings = new org.json.JSONObject(
                "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0} }");

        assertTrue(rValid.createIndexWithSettings(testIndexName, indexSettings));
    }

    @Test
    public void testGetDocument() throws EsRESTException {
        rValid.createIndex(testIndexName);
        assertTrue(rValid.index(testIndexName, testType, "1", testDocument));
        JSONObject doc = rValid.getDocument() //
                .withIndex(testIndexName) //
                .withType(testType) //
                .id("1") //
                .includeSource(false) //
                .fields("age") //
                .sourceOnly(false) //
                .execute(); //
        assertEquals(doc.getString("_id"), "1");
    }

    @Test
    public void getGetDocumentWithoutType() throws EsRESTException {
        rValid.createIndex(testIndexName);
        assertTrue(rValid.index(testIndexName, testType, "1", testDocument));
        JSONObject doc = rValid.getDocument() //
                .withIndex(testIndexName) //
                .id("1") //
                .execute(); //
        assertEquals(doc.getString("_id"), "1");
    }

    @Test
    public void getGetDocumentFromInvalidServer() throws EsRESTException {
        JSONObject doc = rInvalid.getDocument() //
                .withIndex(testIndexName) //
                .withType(testType) //
                .id("1")
                .execute(); //
        assertNull(doc);
    }

    @Test
    public void getGetNonExistingDocument() throws EsRESTException {
        rValid.createIndex(testIndexName);
        JSONObject doc = rValid.getDocument() //
                .withIndex(testIndexName) //
                .id("1") //
                .execute(); //
        assertNull(doc);
    }

    /**
     * <p>
     * testIndexDocWithoutId
     * </p>
     */
    @Test
    public void testIndexDocWithoutId() {
        rValid.createIndex(testIndexName);
        assertTrue(rValid.index(testIndexName, testType, testDocument));
    }

    /**
     * <p>
     * testDeleteIndexThatExists
     * </p>
     */
    @Test
    public void testDeleteIndexThatExists() {
        testCreateIndex();
        assertTrue(rValid.deleteIndex(testIndexName));
    }

    /**
     * <p>
     * testDeleteIndexThatDoesNotExist
     * </p>
     */
    @Test
    public void testDeleteIndexThatDoesNotExist() {
        testCreateIndex();
        assertFalse(rValid.deleteIndex("doesnotexistsforsureright-11234"));
    }

    @Test
    public void testPutMapping() {
        testCreateIndex();
        assertTrue(rValid.putMapping(testIndexName, testType, testMapping));
    }

    /**
     *************************************************************************
     * BULK TESTS
     ***************************************************************************
     */
    @Test
    public void testValidBulkIndex() {
        int bulksize = 50;
        
        rValid.createIndex(testIndexName);
        rValid.setBulkSize(bulksize);

        for (int i = 0; i < bulksize * 20; i++) {
            if (i % bulksize == 0) {
                assertEquals(0, rValid.getCurrentBulkSize());
            }
            
            assertTrue(rValid.bulkIndex(testIndexName, testType,
                    Integer.toString(i), testDocument));
        }
    }

    @Test
    public void testInValidBulkIndex() {
        rInvalid.setBulkSize(20);
        for (int i = 0; i < 19; i++) {
            assertTrue(rInvalid.bulkIndex(testIndexName, testType,
                    Integer.toString(i), testDocument));
        }
        assertFalse(rInvalid.bulkIndex(testIndexName, testType,
                Integer.toString(19), testDocument));
        assertNotEquals(rInvalid.getCurrentBulkSize(), 0);
    }

}
