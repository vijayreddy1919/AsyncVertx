package service;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.client.MockClient;
import io.vertx.core.Handler;
import rx.functions.Action1;
import spock.util.concurrent.AsyncConditions;

import java.io.IOException;

public class MockCouchbaseClient {


    private CouchbaseMock couchbaseMock;
    private AsyncCluster cluster;
    private AsyncBucket bucket;
    private static boolean initialized = false;
private static MockCouchbaseClient instance = null;
    private AsyncConditions asyncConditions = new AsyncConditions();


    private MockCouchbaseClient()  {

super();

    }

    public static MockCouchbaseClient getInstance(){

        if(instance==null) {

            instance = new MockCouchbaseClient();

        }

        return instance;
        }

    public void createClient(Action1<AsyncBucket> handler) throws IOException, InterruptedException {


         couchbaseMock = MockCouchbaseServer.getInstance().getCouchbaseMock();

        int httpPort = couchbaseMock.getHttpPort();
        int carrierPort = couchbaseMock.getCarrierPort("default");

        cluster = CouchbaseAsyncCluster.create(DefaultCouchbaseEnvironment.builder()
            .bootstrapCarrierDirectPort(carrierPort)
            .bootstrapHttpDirectPort(httpPort)
            .build(), "couchbase://127.0.0.1");

        cluster.openBucket("default").doOnError(throwable ->
            System.out.println("Unable to open bucket")
        ).subscribe(handler,


            err ->
                System.out.println("Exception in bucket creation = " + err.getCause())

        );

    }

    public void destroy(){
        if (cluster != null) {
            cluster.disconnect();
        }
        if (couchbaseMock != null) {
            couchbaseMock.stop();
        }

    }





}
