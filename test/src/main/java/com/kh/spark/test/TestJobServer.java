/**
 * 
 */
package com.kh.spark.test;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.wink.json4j.JSON;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;

/**
 * @author nguyenbh
 *
 */
public class TestJobServer extends TestCase {

	static private String _scheme = "http";
//	static private String _HostName = "sdsvm923078.svl.ibm.com";
	static private String _HostName = "sdsvm923079.svl.ibm.com";
//	static private String _HostName = "ec2-54-226-97-140.compute-1.amazonaws.com";
	static private String _Port = "8090";
	static private String _jobServerDirectory = "/home/nguyenbh/job-server";
//	static private String _jobServerDirectory = "/home/ec2-user/job-server";
	static private String _context = "kh-context";
	
	static private HttpClient  _httpClient = HttpClientBuilder.create().build();

	/**
	 * @param name
	 */
	public TestJobServer(String name) {
		super(name);
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		if( !this.contextExists(_context) ) {
			this.createContext(_context);
		}
		System.out.println("Begin " + this.getName());
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
		System.out.println("End " + this.getName() + "\n");
	}

	public void testKHMasterJob() {
		try {
			String path = "jobs";
			String data = "file.type=text,file.path=README.md";

			this.post(path, "KHMasterJob", null, data, "OK");
						
		} catch (Exception e) {
			fail(e.getMessage());
		}		
	}
	
	public void testKHMasterJobToCountAllWords() {
		try {
			String path = "jobs";
			String data = "file.type=text,file.path=README.md,count.type=word";

			this.post(path, "KHMasterJob", null, data, "OK");

		} catch (Exception e) {
			fail(e.getMessage());
		}		
	}
	
	public void testKHMasterJobToCountWordSpark() {
		try {
			String path = "jobs";
			String data = "file.type=text,file.path=README.md,count.type=word,count.word=Spark";

			this.post(path, "KHMasterJob", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}		
	}

	public void testKHJob() {
		try {
			String path = "jobs";
			String data = "";

			this.post(path, "KHJob", null, data, "OK");

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testWebPageJob() {
		try {
			String path = "jobs";
			String data = "keyword=apple";

			this.post(path, "WebPageJob", null, data, "OK");

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testJoin() {
		try {
			// Orders
			String path = "jobs";
			String ordersData = "rdd.name=orders,table.name=orders,file.path=" + _jobServerDirectory + "/db/datafiles/Orders.txt,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"orderDate\", \"timestamp\", \"false\"],[\"requiredDate\", \"timestamp\", \"false\"],[\"shippedDate\", \"timestamp\", \"false\"],[\"status\", \"string\", \"false\"],[\"comments\", \"string\", \"true\"],[\"customerNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, ordersData, "OK");

			// OrderDetails
			String detailsData = "rdd.name=details,table.name=details,file.path=" + _jobServerDirectory + "/db/datafiles/OrderDetails.txt,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"productCode\", \"string\", \"false\"],[\"quantityOrdered\", \"integer\", \"false\"],[\"priceEach\", \"double\", \"false\"],[\"orderLineNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, detailsData, "OK");
			
			// Join Orders with OrderDetails
			String joinData = "left.rdd.name=orders,right.rdd.name=details,join.type=\"INNER JOIN\",left.key=orderNumber,right.key=orderNumber";
			
			this.post(path, "Join", null, joinData, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testSQLExecutor() {
		try {
			// Orders
			String path = "jobs";
			String ordersData = "rdd.name=orders,table.name=orders,file.path=" + _jobServerDirectory + "/db/datafiles/Orders.txt,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"orderDate\", \"timestamp\", \"false\"],[\"requiredDate\", \"timestamp\", \"false\"],[\"shippedDate\", \"timestamp\", \"false\"],[\"status\", \"string\", \"false\"],[\"comments\", \"string\", \"true\"],[\"customerNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, ordersData, "OK");

			String data = "rdd.name=orders,sql=\"SELECT orderNumber, customerNumber FROM orders\"";
			this.post(path, "SQLExecuter", null, data, "OK");

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testTextFile() {
		try {
			String path = "jobs";
			String data = "file.path=README.md,rdd.name=README";

			this.post(path, "TextFile", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testTextFileNoFilePath() {
		try {
			String path = "jobs";
			String data = "rdd.name=README";

			this.post(path, "TextFile", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
	}
	
	public void testCountTextFile() {
		try {
			String path = "jobs";
			String data = "file.path=README.md,rdd.name=README";

			this.post(path, "TextFile", null, data, "OK");

			// Use RDD to count word
			data = "rdd.name=README,count.word=Spark";
			
			this.post(path, "Counter", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
	}

	public void testCountTextFileNoRDD() {
		try {
			String path = "jobs";
			String data = "file.path=README.md,rdd.name=README";

			this.post(path, "TextFile", null, data, "OK");

			// Use RDD to count word
			data = "count.word=Spark";
			
			this.post(path, "Counter", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
	}

	public void testWebPageMiner() {
		try {
			String path = "jobs";
			String data = "keyword=ebola";

			this.post(path, "WebPageMiner", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testWebPageMinerNoKeyWord() {
		try {
			String path = "jobs";
			String data = "";

			this.post(path, "WebPageMiner", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testWebPageMinerAndReadFirstPage() {
		try {
			String path = "jobs";
			String data = "keyword=ebola";

			JSONObject response = this.post(path, "WebPageMiner", null, data, "OK");
			JSONArray result = response.getJSONArray("result");
			JSONArray firstEntry = result.getJSONArray(0);
			String firstURL = firstEntry.getString(0);

			// Read first page
			data = "rdd.name=apple,url=\"" + firstURL +"\"";
			this.post(path, "WebPageReader", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testWebPageMinerAndReadFirstPageNoURL() {
		try {
			String path = "jobs";
			String data = "keyword=iphone";

			this.post(path, "WebPageMiner", null, data, "OK");

			// Read first page
			data = "rdd.name=apple";
			this.post(path, "WebPageReader", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testWebPageMinerAndReadFirstPageThenCount() {
		try {
			String path = "jobs";
			String data = "keyword=ebola";

			JSONObject response = this.post(path, "WebPageMiner", null, data, "OK");
			JSONArray result = response.getJSONArray("result");
			JSONArray firstEntry = result.getJSONArray(0);
			String firstURL = firstEntry.getString(0);

			// Read first page
			data = "rdd.name=ebola,url=\"" + firstURL +"\"";
			this.post(path, "WebPageReader", null, data, "OK");
			
			// Count the word problem
			data = "rdd.name=ebola,count.word=ebola";
			this.post(path, "Counter", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testTwitterReader() {
		try {
			String path = "jobs";
			String data = "key=\"49ers\",rdd.name=niners";

			this.post(path, "TwitterReader", null, data, "OK");

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testTwitterReaderNoKey() {
		try {
			String path = "jobs";
			String data = "rdd.name=niners";

			this.post(path, "TwitterReader", null, data, "VALIDATION FAILED");

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testCountTweets() {
		try {
			String path = "jobs";
			String data = "key=ebola,rdd.name=niners";

			this.post(path, "TwitterReader", null, data, "OK");

			// Use RDD to count word
			data = "rdd.name=niners,count.word=the";
			
			this.post(path, "Counter", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testCountTweetsNoRDD() {
		try {
			String path = "jobs";
			String data = "key=\"49ers\",rdd.name=niners";

			this.post(path, "TwitterReader", null, data, "OK");

			// Use RDD to count word
			data = "count.word=the";
			
			this.post(path, "Counter", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testSequenceFileOrders() {
		try {
			String path = "jobs";
			String data = "rdd.name=orders,table.name=orders,file.path=" + _jobServerDirectory + "/db/datafiles/Orders.txt,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"orderDate\", \"timestamp\", \"false\"],[\"requiredDate\", \"timestamp\", \"false\"],[\"shippedDate\", \"timestamp\", \"false\"],[\"status\", \"string\", \"false\"],[\"comments\", \"string\", \"true\"],[\"customerNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testSequenceFileOrdersNoTableName() {
		try {
			String path = "jobs";
			String data = "rdd.name=orders,file.path=" + _jobServerDirectory + "/db/datafiles/Orders.txt,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"orderDate\", \"timestamp\", \"false\"],[\"requiredDate\", \"timestamp\", \"false\"],[\"shippedDate\", \"timestamp\", \"false\"],[\"status\", \"string\", \"false\"],[\"comments\", \"string\", \"true\"],[\"customerNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testSequenceFileOrdersNoFilePath() {
		try {
			String path = "jobs";
			String data = "rdd.name=orders,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"orderDate\", \"timestamp\", \"false\"],[\"requiredDate\", \"timestamp\", \"false\"],[\"shippedDate\", \"timestamp\", \"false\"],[\"status\", \"string\", \"false\"],[\"comments\", \"string\", \"true\"],[\"customerNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testSequenceFileOrdersNoSchema() {
		try {
			String path = "jobs";
			String data = "rdd.name=orders,table.name=orders,file.path=" + _jobServerDirectory + "/db/datafiles/Orders.txt";

			this.post(path, "SequentialFile", null, data, "VALIDATION FAILED");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void testSequenceFileOrderDetails() {
		try {
			String path = "jobs";
			String data = "rdd.name=details,table.name=details,file.path=" + _jobServerDirectory + "/db/datafiles/OrderDetails.txt,schema=[[\"orderNumber\", \"integer\", \"false\"],[\"productCode\", \"string\", \"false\"],[\"quantityOrdered\", \"integer\", \"false\"],[\"priceEach\", \"double\", \"false\"],[\"orderLineNumber\", \"integer\", \"false\"]]";

			this.post(path, "SequentialFile", null, data, "OK");
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	

	

	
	protected void createContext(String context) {
		try {
			URIBuilder builder = new URIBuilder();
			
			builder.setScheme(_scheme).setHost(_HostName).setPort(Integer.parseInt(_Port)).setPath("/contexts/" + context);
			
			URI uri = builder.build();
			HttpPost httpPost = new HttpPost(uri);
			
			Map<String, String> headers = new HashMap<String, String>();
			headers.put("Content-Type", "application/x-www-form-urlencoded");
			
			if( headers != null ) {
				for (Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator(); iterator.hasNext();) {
					Map.Entry<String, String> headerSet = (Map.Entry<String, String>) iterator.next();
					httpPost.setHeader(headerSet.getKey(), headerSet.getValue());
				}
			}
			
			String data = "";
			StringEntity iEntity = new StringEntity(data);
			httpPost.setEntity(iEntity);			
			
			HttpResponse response = _httpClient.execute(httpPost);
			assertNotSame("Request failed", 200, response.getStatusLine().getStatusCode());
			
			HttpEntity oEntity = response.getEntity();
			EntityUtils.consume(oEntity);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	protected boolean contextExists(String context) {
		boolean exists = false;
		try {
			URIBuilder builder = new URIBuilder();
			
			builder.setScheme(_scheme).setHost(_HostName).setPort(Integer.parseInt(_Port)).setPath("/contexts");
			
			URI uri = builder.build();
			HttpGet httpGet = new HttpGet(uri);
			
			HttpResponse response = _httpClient.execute(httpGet);
			assertNotSame("Request failed", 200, response.getStatusLine().getStatusCode());
			
			HttpEntity entity = response.getEntity();
			JSONArray contexts = (JSONArray)JSON.parse(entity.getContent());
			EntityUtils.consume(entity);
			if( contexts != null ) {
				for( int i = 0; i < contexts.size(); i++ ) {
					if( contexts.getString(0).equalsIgnoreCase(context) ) {
						return true;
					}
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return exists;		
	}
	
	protected JSONObject post(String path, String jobName, Map<String, String> headers, String data, String expectedStatus) {
		try {
			URIBuilder builder = new URIBuilder();
			
			builder.setScheme(_scheme).setHost(_HostName).setPort(Integer.parseInt(_Port)).setPath("/" + path);
			
			Map<String, String> queryParams = new HashMap<String, String>();
			queryParams.put("appName", "kh");
			queryParams.put("classPath", "com.kh.spark." + jobName);
			queryParams.put("sync", "true");
			queryParams.put("context", _context);

			for (Iterator<Map.Entry<String, String>> iterator = queryParams.entrySet().iterator(); iterator.hasNext();) {
				Map.Entry<String, String> qParamSet = iterator.next();
				builder.setParameter(qParamSet.getKey(), qParamSet.getValue());
			}

			URI uri = builder.build();
			HttpPost httpPost = new HttpPost(uri);
						
			if( headers == null ) {
				headers = new HashMap<String, String>();
				headers.put("Content-Type", "application/x-www-form-urlencoded");
			}
			
			for (Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator(); iterator.hasNext();) {
				Map.Entry<String, String> headerSet = (Map.Entry<String, String>) iterator.next();
				httpPost.setHeader(headerSet.getKey(), headerSet.getValue());
			}
			
			// Data
			StringEntity iEntity = new StringEntity(data);
			httpPost.setEntity(iEntity);			
			
			HttpResponse response = _httpClient.execute(httpPost);
			assertNotSame("Request failed", 200, response.getStatusLine().getStatusCode());
			
			HttpEntity oEntity = response.getEntity();
			JSONObject result = (JSONObject)JSON.parse(oEntity.getContent());
			System.out.println(result.toString());
			String status = result.getString("status");
			assertEquals("Unexpected status", expectedStatus, status);
			
			EntityUtils.consume(oEntity);
			
			return result;
		} catch (Exception e) {
			fail(e.getMessage());
		}
		
		return null;
	}

//	private String getMethodName(final int depth)	{
//	  final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
//
//	  return ste[ste.length - 1 - depth].getMethodName(); //Thank you Tom Tresansky
//	}
	
}
