package service;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;

import java.io.IOException;
import java.util.ArrayList;

public class MockCouchbaseServer {

    public static String NAME = "default";
    public static String PASSWORD = "";
    private static MockCouchbaseServer instance = null;
    private final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    private CouchbaseMock couchbaseMock = null;

    private static boolean initialized = false;

    public void startServer() throws IOException, InterruptedException {

        bucketConfiguration.numNodes = 1;
        bucketConfiguration.numReplicas = 1;
        bucketConfiguration.numVBuckets = 1024;
        bucketConfiguration.name = NAME;
        bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
        bucketConfiguration.password = PASSWORD;
        ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        couchbaseMock = new CouchbaseMock(0, configList);
        couchbaseMock.start();
        couchbaseMock.waitForStartup();


    }

    private MockCouchbaseServer() throws IOException, InterruptedException {

        startServer();
    }

    public static MockCouchbaseServer getInstance() throws IOException, InterruptedException {

        if(instance==null){

            instance = new MockCouchbaseServer();

        }


            return instance;

    }


    public CouchbaseMock getCouchbaseMock() {
        return couchbaseMock;
    }

    public void setCouchbaseMock(CouchbaseMock couchbaseMock) {
        this.couchbaseMock = couchbaseMock;
    }

    public void destroy(){
        if (couchbaseMock != null) {
            couchbaseMock.stop();
        }
    }

}
