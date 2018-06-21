package service;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import io.vertx.core.json.JsonObject;

public class Driver
{
static CouchbaseRepositoryWrapper repo = new CouchbaseRepositoryWrapper();
static AsyncBucket bucket = null;
public static void main(String args[]){
    JsonObject config = new JsonObject();
    config.put("username","user");
    config.put("password","password");
    config.put("bucketName", "beer-sample");
    config.put("hostName","localhost");
    repo.getConnection(config, res-> {
        if(res.succeeded()){
          bucket = res.result();
        }
        if(res.failed()){
            System.out.println("Failed!");
        }
    });


    try
    {
        Thread.sleep(10000);
    }
    catch(InterruptedException ex)
    {
        Thread.currentThread().interrupt();
    }

    System.out.println(bucket);

    com.couchbase.client.java.document.json.JsonObject jsonObject = com.couchbase.client.java.document.json.JsonObject.create().put("Name","Shravan");
    JsonDocument document = JsonDocument.create("aaa", jsonObject);
    repo.upsertDocument(bucket, document, res -> {
        if(res.succeeded()){
            System.out.println(res.result());
        }
        if(res.failed()){
            System.out.println("Failed to update!!!");
        }

        }
    );






    try
    {
        Thread.sleep(1000);
    }
    catch(InterruptedException ex)
    {
        Thread.currentThread().interrupt();
    }




}

}
