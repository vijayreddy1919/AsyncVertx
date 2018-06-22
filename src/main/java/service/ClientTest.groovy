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
import org.junit.Test
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions;

import java.util.ArrayList;

public class Client extends Specification {
    protected final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    protected MockClient mockClient;
    protected CouchbaseMock couchbaseMock;
    protected AsyncCluster cluster;
    protected AsyncBucket bucket;
    protected int carrierPort;
    protected int httpPort;

    private AsyncConditions asyncConditions;


    def getPortInfo(String bucket) throws Exception {
        httpPort = couchbaseMock.getHttpPort();
        carrierPort = couchbaseMock.getCarrierPort(bucket);
    }

    def createMock(@NotNull String name, @NotNull String password) throws Exception {
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

    def createClient() {
        cluster = CouchbaseAsyncCluster.create(DefaultCouchbaseEnvironment.builder()
            .bootstrapCarrierDirectPort(carrierPort)
            .bootstrapHttpDirectPort(httpPort)
            .build() ,"couchbase://127.0.0.1");

    cluster.openBucket("default").doOnError({ throwable ->
        System.out.println("Unable to open bucket");

    })
        .subscribe({ asyncBucket -> bucket = asyncBucket ; asyncConditions.evaluate {bucket!=null};},
        { err ->
            System.out.println("Exception in bucket creation = " + err.getCause());
        });

    asyncConditions.await(5);


 println 'After await'

   /*     try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
*/

        //  bucket = cluster.openBucket("default");
    }

    def setup() throws Exception {

asyncConditions = new AsyncConditions();
        createMock("default", "");
        getPortInfo("default");
        createClient();
    }


    def tearDown() throws Exception {
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


    def testSimple() {

        given:
        final String[] s = {"unchanged"};
        // bucket.upsert(JsonDocument.create("foo")).subscribe({jsonDocument -> System.out.print(jsonDocument + "!"); s[0] = "changed";});

        when:
        bucket.upsert(JsonDocument.create("foo")).subscribe({jsonDocument -> asyncConditions.evaluate {jsonDocument!=null}; System.out.print(jsonDocument.toString() + "!" ); s[0] = "changed";});

        bucket.get(JsonDocument.create("foo")).subscribe({jsonDocument -> asyncConditions.evaluate {jsonDocument!=null}; System.out.print(jsonDocument); s[0] = "changed";});

        System.out.print(s[0]);

        then:

asyncConditions.await()
        1==1
    }



    def testSimple2() {





        given:

        CouchbaseRepositoryWrapper repo = new CouchbaseRepositoryWrapper();
        io.vertx.core.json.JsonObject config = new io.vertx.core.json.JsonObject();
        config.put("username","default")
        config.put("password", "")
        config.put("bucketName","default")
        config.put("hostName", couchbaseMock.getHttpPort().toString())
        when:

        AsyncBucket asyncBucket = null;
repo.getConnection(config, {

    async ->
          if (async.succeeded()) {
            asyncBucket = async.result();
            print asyncBucket

        }
        if (async.failed()) {
            print 'failed to open connection!!'

        }


})

        println asyncBucket
println 'before'


        Thread.sleep(10000);

        asyncConditions.await()
        println 'after'

        repo.createDocument(asyncBucket, JsonDocument.create("aaaaaa"), {async -> if(async.succeeded()){
            print async.result();
            asyncConditions.evaluate { async.result()!=null}
        }
            if(async.failed()){}

            print 'failed to open connection!!'

        } )
        then:


        asyncConditions.await()
        1==1
    }
}
