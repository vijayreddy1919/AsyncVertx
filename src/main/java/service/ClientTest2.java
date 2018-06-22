

package service;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.JsonUtils;
import com.couchbase.mock.client.MockClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import junit.framework.TestCase;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.ArrayList;

public class ClientTest2 extends TestCase {
    protected final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    protected MockClient mockClient;
    protected CouchbaseMock couchbaseMock;
    protected Cluster cluster;
    protected com.couchbase.client.java.Bucket bucket;
    protected int carrierPort;
    protected int httpPort;

    protected void getPortInfo(String bucket) throws Exception {
        httpPort = couchbaseMock.getHttpPort();
        carrierPort = couchbaseMock.getCarrierPort(bucket);
    }

    protected void createMock(@NotNull String name, @NotNull String password) throws Exception {
        bucketConfiguration.numNodes = 1;
        bucketConfiguration.numReplicas = 1;
        bucketConfiguration.numVBuckets = 1024;
        bucketConfiguration.name = name;
        bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
        bucketConfiguration.password = password;
        ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        couchbaseMock = new CouchbaseMock(0, configList);
        couchbaseMock.start();
        couchbaseMock.waitForStartup();
    }

    protected void createClient() {
        cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder()
            .bootstrapCarrierDirectPort(carrierPort)
            .bootstrapHttpDirectPort(httpPort)
            .build() ,"couchbase://127.0.0.1");
        bucket = cluster.openBucket("default");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        createMock("default", "");
        getPortInfo("default");
        createClient();
    }

    @Override
    protected void tearDown() throws Exception {
        if (cluster != null) {
            cluster.disconnect();
        }
        if (couchbaseMock != null) {
            couchbaseMock.stop();
        }
        if (mockClient != null) {
            mockClient.shutdown();
        }
        super.tearDown();
    }

    @Test
    public void testSimple() {
        bucket.upsert(JsonDocument.create("foo"));
       JsonDocument doc =  bucket.get(JsonDocument.create("foo"));
       System.out.print(doc);
    }
}
