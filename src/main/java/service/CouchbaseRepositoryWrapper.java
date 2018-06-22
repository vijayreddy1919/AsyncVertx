package service;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CouchbaseAsyncCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class CouchbaseRepositoryWrapper {

    public void getConnection(io.vertx.core.json.JsonObject config, Handler<AsyncResult<AsyncBucket>> readyHandler) {
        AsyncCluster cluster = CouchbaseAsyncCluster.create(config.getString("hostName"));
        cluster.authenticate(config.getString("username"), config.getString("password"))
            .openBucket(config.getString("becketName"))
            .doOnError(throwable -> {
                System.out.println("Unable to open bucket");
            })
            .subscribe(asyncBucket -> readyHandler.handle(Future.succeededFuture(asyncBucket)),
                err -> {
                    System.out.println("Exception in bucket creation = " + err.getCause());
                    readyHandler.handle(Future.failedFuture("Unable to connect to Couchbase bucket: " + err.getCause()));
                });
    }

    public void getAllDocuments(AsyncBucket asyncBucket, N1qlQuery query, Handler<AsyncResult<io.vertx.core.json.JsonArray>> readyHandler) {

        executeN1qlQuery(asyncBucket, query, res -> {
            if (res.succeeded()) {
                readyHandler.handle(Future.succeededFuture(res.result()));
            } else {
                readyHandler.handle(Future.failedFuture("Unable to retrieve documents : " + res.cause()));
            }
        });
    }

    private void executeN1qlQuery(AsyncBucket bucket, N1qlQuery query, Handler<AsyncResult<io.vertx.core.json.JsonArray>> readyHandler) {
        io.vertx.core.json.JsonArray documents = new io.vertx.core.json.JsonArray();
        bucket.query(query)
            .doOnError(throwable -> {
                //System.out.println("query processing failed..." + throwable);
                readyHandler.handle(Future.failedFuture("Unable to get the result"));
            })
            .subscribe(asyncN1qlQueryResult -> {
                //System.out.println("query result = " + asyncN1qlQueryResult.rows());
                asyncN1qlQueryResult.rows().forEach(row -> {
                    //System.out.println("updated document =" + row.value().getObject(bucket.name()));
                    io.vertx.core.json.JsonObject jsonDocument = new io.vertx.core.json.JsonObject(row.value().getObject(bucket.name()).toString());
                    documents.add(jsonDocument);
                }, er -> {
                    //System.out.println("In getAllDocuments Failure handler...");
                    readyHandler.handle(Future.failedFuture("Unable to process the result"));
                }, () -> {
                    //System.out.println("In getAllDocuments success handler...");
                    readyHandler.handle(Future.succeededFuture(documents));
                });
            }, err -> {
                //System.out.println("Subscribe Failure handler...");
                readyHandler.handle(Future.failedFuture("Unable to process the result"));
            });
    }

    public void getDocumentByN1QLQuery(AsyncBucket asyncBucket, N1qlQuery query, Handler<AsyncResult<io.vertx.core.json.JsonArray>> readyHandler) {
        executeN1qlQuery(asyncBucket, query, res -> {
            if (res.succeeded()) {
                readyHandler.handle(Future.succeededFuture(res.result()));
            } else {
                readyHandler.handle(Future.failedFuture("Unable to retrieve document"));
            }
        });
    }

    public void getDocumentByKey(AsyncBucket asyncBucket, String id, Handler<AsyncResult<JsonObject>> readyHandler) {
        asyncBucket.get(id)
            .subscribe(doc -> {
                if(doc != null) {
                    com.couchbase.client.java.document.json.JsonObject document = doc.content();
                    readyHandler.handle(Future.succeededFuture(document));
                } else {
                    readyHandler.handle(Future.failedFuture("No results found"));
                }
            }, err -> {readyHandler.handle(Future.failedFuture("Exception = " + err.getCause()));});
    }

    public void createDocumentByN1QLQuery(AsyncBucket asyncBucket, N1qlQuery query, Handler<AsyncResult<io.vertx.core.json.JsonArray>> readyHandler) {
        executeN1qlQuery(asyncBucket, query, res -> {
            if (res.succeeded()) {
                readyHandler.handle(Future.succeededFuture(res.result()));
            } else {
                readyHandler.handle(Future.failedFuture("Unable to create document"));
            }
        });
    }

    public void createDocument(AsyncBucket asyncBucket, JsonDocument document, Handler<AsyncResult<JsonObject>> readyHandler) {
        // id ????
        asyncBucket.insert(document)
            .subscribe(doc -> {
                    readyHandler.handle(Future.succeededFuture(doc.content()));
                },
                err -> {readyHandler.handle(Future.failedFuture("Exception: Unable to create document - " + err.getCause()));});
    }

    public void updateDocumentByN1QLQuery(AsyncBucket asyncBucket, N1qlQuery query, Handler<AsyncResult<io.vertx.core.json.JsonArray>> readyHandler) {
        executeN1qlQuery(asyncBucket, query, res -> {
            if (res.succeeded()) {
                readyHandler.handle(Future.succeededFuture(res.result()));
            } else {
                readyHandler.handle(Future.failedFuture("Unable to retrieve document"));
            }
        });
    }

    public void updateDocument(AsyncBucket asyncBucket, JsonDocument document, Handler<AsyncResult<JsonObject>> readyHandler) {
        asyncBucket.replace(document)
            .doOnError(throwable -> {
                //System.out.println("Exception in query execution " + throwable);
                if (throwable instanceof DocumentDoesNotExistException) {
                    readyHandler.handle(Future.failedFuture("Document does not exist"));
                }
            })
            .subscribe(doc -> {
                //System.out.println("Error occurred in handling update result");
                readyHandler.handle(Future.succeededFuture(doc.content()));
            }, err -> {
                //System.out.println("Error occurred in handling update result");
                readyHandler.handle(Future.failedFuture("Exception: Unable to update document - " + err.getCause()));
            });
    }

    public void upsertDocument(AsyncBucket asyncBucket, JsonDocument document, Handler<AsyncResult<JsonObject>> readyHandler) {
        asyncBucket.upsert(document)
            .subscribe(doc -> {
                readyHandler.handle(Future.succeededFuture(doc.content()));
            }, err -> {readyHandler.handle(Future.failedFuture("Exception: Unable to update document - " + err.getCause()));});
    }

    public void deleteDocumentByN1QLQuery(AsyncBucket asyncBucket, N1qlQuery query, Handler<AsyncResult<io.vertx.core.json.JsonArray>> readyHandler) {
        executeN1qlQuery(asyncBucket, query, res -> {
            if (res.succeeded()) {
                readyHandler.handle(Future.succeededFuture(res.result()));
            } else {
                readyHandler.handle(Future.failedFuture("Unable to retrieve document"));
            }
        });
    }

    public void deleteDocumentByKey(AsyncBucket asyncBucket, String id, Handler<AsyncResult<String>> readyHandler) {
        asyncBucket.remove(id)
            .subscribe(doc -> {
            }, err -> {
                //System.out.println("Unable to delete document");
                readyHandler.handle(Future.failedFuture("Unable to delete document = " + err));
            }, () -> readyHandler.handle(Future.succeededFuture("Delete success")));
    }

    public void deleteDocument(AsyncBucket asyncBucket, JsonDocument document, Handler<AsyncResult<String>> readyHandler) {
        asyncBucket.remove(document)
            .subscribe(doc -> {
            }, err -> {
                //System.out.println("Unable to delete document");
                readyHandler.handle(Future.failedFuture("Exception: Unable to delete document = "));
            }, () -> {
                readyHandler.handle(Future.succeededFuture("Delete success"));
            });
    }
}
