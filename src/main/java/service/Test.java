package service;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import spock.util.concurrent.AsyncConditions;


import java.io.IOException;

public class Test {

    static CouchbaseRepositoryWrapper repo = new CouchbaseRepositoryWrapper();
    static AsyncBucket bucket = null;
static AsyncConditions asyncConditions = new AsyncConditions();
    public static void main(String args[]) throws Throwable {

        int test = 1;
        MockCouchbaseClient client = MockCouchbaseClient.getInstance();
        client.createClient( asyncBucket -> {asyncConditions.evaluate(() -> { bucket = asyncBucket;}); });
        asyncConditions.await(25);

       JsonDocument doc =  JsonDocument.create("moo", JsonObject.create().put("Name","LLLL"));
        repo.upsertDocument(bucket, doc, event -> {
            if(event.succeeded())
        {
            System.out.println("Insertion done");
        }


        });

        Thread.sleep(3000);

       /* bucket.get("moo").subscribe(jsonDocument -> {
          System.out.println(jsonDocument + "!");
        });*/


        repo.getDocumentByKey(bucket, "moo", event -> {
            if(event.succeeded()){
                System.out.println(event.result().toString() + "......");
            }
            if(event.failed()){
                System.out.println(event.cause());
            }

        });

        System.out.println(bucket);
        bucket.upsert(JsonDocument.create("foo")).subscribe(jsonDocument -> System.out.println(jsonDocument));
        Thread.sleep(50000);
System.out.println("Done");
    }
}
