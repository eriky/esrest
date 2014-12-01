package nl.test.esresty;

import static org.junit.Assert.*;

import org.json.JSONObject;
import org.junit.Test;

import com.eriky.EsRESTException;
import com.eriky.requests.Get;

public class GetTests {
    String testUrl = "http://localhost:9200";
    String testIndexName = "testindex";
    String testType = "testtype";
    String testId = "testid";
    
    @Test
    public void testUrlCreation() {
        Get get = new Get(testUrl).withIndex(testIndexName).withType(testType).id(testId);
        assertEquals(get.getUrl(), testUrl + '/' + testIndexName + '/' + testType + '/' + testId);
    }
    @Test
    public void testId() throws EsRESTException {
        Get get = new Get(testUrl).withIndex(testIndexName).withType(testType).id("testid");
        JSONObject result = get.execute();
        assertEquals(result, null);
    }

    @Test
    public void testSourceOnly() {
        Get get = new Get(testUrl).sourceOnly(true);
        assertTrue(get.getUrl().endsWith("_source"));
    }

    @Test
    public void testRouting() {
        Get get = new Get(testUrl).routing("testrouting");
        assertEquals(get.getQueryString("routing"), "testrouting");
    }

    @Test
    public void testIncludeSource() {
        Get get = new Get(testUrl).includeSource(true);
        assertTrue((Boolean)get.getQueryString("source"));
    }

    @Test
    public void testFields() {
        String testFields = "a,b,c";
        Get get = new Get(testUrl).fields(testFields);
        assertEquals(get.getQueryString("fields"), testFields);
    }

    @Test
    public void testPreference() {
        String preference = "_primary";
        Get get = new Get(testUrl).preference(preference);
        assertEquals(get.getQueryString("preference"), preference);
    }

    @Test
    public void testRefresh() {
        Get get = new Get(testUrl).refresh(true);
        assertTrue((Boolean)get.getQueryString("refresh"));
    }

    @Test
    public void testVersion() {
        String version = "1";
        Get get = new Get(testUrl).version(version);
        assertEquals(get.getQueryString("version"), version);
    }
}
