package service;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.JsonUtils;
import com.couchbase.mock.client.MockClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.vertx.core.Future;
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

public class ClientTest extends TestCase {
    protected final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    protected MockClient mockClient;
    protected CouchbaseMock couchbaseMock;
    protected AsyncCluster cluster;
    protected AsyncBucket bucket;
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
        cluster = CouchbaseAsyncCluster.create(DefaultCouchbaseEnvironment.builder()
            .bootstrapCarrierDirectPort(carrierPort)
            .bootstrapHttpDirectPort(httpPort)
            .build() ,"couchbase://127.0.0.1");

        cluster.openBucket("default").doOnError(throwable -> {
            System.out.println("Unable to open bucket");
        })
            .subscribe(asyncBucket -> bucket = asyncBucket,
                err -> {
                    System.out.println("Exception in bucket creation = " + err.getCause());
                });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //  bucket = cluster.openBucket("default");
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

        final String[] s = {"unchanged"};
        bucket.upsert(JsonDocument.create("foo")).subscribe(jsonDocument -> {System.out.print(jsonDocument + "!"); s[0] = "changed";});
        bucket.get(JsonDocument.create("foo")).subscribe(jsonDocument -> {System.out.print(jsonDocument); s[0] = "changed";});

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {


            System.out.print("errorrrrrrrrrrrrrrrrrrrrrrrrrrrr");
            e.printStackTrace();

        }
 System.out.print(s[0]);

    }
}
