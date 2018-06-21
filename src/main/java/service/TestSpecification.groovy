package service

import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.document.JsonDocument
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions

class TestSpecification extends Specification{

    private AsyncConditions asyncConditions

    private AsyncBucket bucket = null

    private CouchbaseRepositoryWrapper repo = new CouchbaseRepositoryWrapper();

    private String key = "bbb";
    def setup() {

        asyncConditions = new AsyncConditions()


        JsonObject config = new JsonObject();
        config.put("username","user");
        config.put("password","password");
        config.put("bucketName", "beer-sample");
        config.put("hostName","localhost");
        if(bucket == null) {

            repo.getConnection(config, new Handler<AsyncResult<JsonArray>>() {


                @Override
                void handle(AsyncResult<JsonArray> event) {

                    asyncConditions.evaluate {

                        if (event.succeeded()) {
                            bucket = event.result();

                        }
                        if (event.failed()) {
                            System.out.println("Failed!");
                        }

                    }
                }


            });
        }
        asyncConditions.await();
    }

    def "Upsert a record into db"() {

        given:

        com.couchbase.client.java.document.json.JsonObject jsonObject = com.couchbase.client.java.document.json.JsonObject.create().put("Name","Shravan");
        JsonDocument document = JsonDocument.create(key, jsonObject);

        when:

      repo.upsertDocument(bucket, document, new Handler<AsyncResult<com.couchbase.client.java.document.json.JsonObject>>() {
          @Override
          void handle(AsyncResult<com.couchbase.client.java.document.json.JsonObject> event) {
              asyncConditions.evaluate {
                  if (event.succeeded()) {
                      System.out.println(res.result());
                  }
                  if (event.failed()) {
                      System.out.println("Failed to update!!!");
                  }
              }


          }

        }
        );

asyncConditions.await()
        then:
        1==1

    }


    def "Get document by key"() {

        given:


        when:

repo.getDocumentByKey(bucket, key, new Handler<AsyncResult<com.couchbase.client.java.document.json.JsonObject>>() {
    @Override
    void handle(AsyncResult<com.couchbase.client.java.document.json.JsonObject> event) {

        asyncConditions.evaluate {
            if (event.succeeded()) {
                System.out.println(res.result());
            }
            if (event.failed()) {
                System.out.println("Failed to fetch!!!");
            }
        }

    }
})

        asyncConditions.await();
        then:
        1==1

        Thread.sleep(10000);
    }

}
