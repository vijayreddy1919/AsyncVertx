package service;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import spock.util.concurrent.AsyncConditions;


import java.io.IOException;

public class Test {

    static AsyncBucket bucket = null;
static AsyncConditions asyncConditions = new AsyncConditions();
    public static void main(String args[]) throws Throwable {

        int test = 1;
        MockCouchbaseClient client = MockCouchbaseClient.getInstance();
        client.createClient( asyncBucket -> {asyncConditions.evaluate(() -> {System.out.print("x"); }); bucket = asyncBucket; });

        System.out.println(bucket);
        bucket.upsert(JsonDocument.create("foo")).subscribe(jsonDocument -> System.out.println(jsonDocument));
        Thread.sleep(50000);
System.out.println("Done");
asyncConditions.await(25);
    }
}
