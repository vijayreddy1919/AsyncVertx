package service

import com.couchbase.client.core.message.stat.Stat
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.AsyncCluster
import com.couchbase.client.java.CouchbaseAsyncCluster
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.AsyncN1qlQueryResult
import com.couchbase.client.java.query.AsyncN1qlQueryRow
import com.couchbase.client.java.query.N1qlQuery
import mockit.MockUp
import rx.Observable
import rx.Subscriber
import rx.functions.Action1
import service.CouchbaseRepositoryWrapper
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions
import service.CouchbaseRepositoryWrapper


class CouchbaseRepositoryWrapperSpecification extends Specification {

     private AsyncConditions asyncConditions
    private AsyncBucket bucket
    private CouchbaseRepositoryWrapper wrapper = new CouchbaseRepositoryWrapper();
    private JsonDocument doc
    private JsonDocument failDoc
private static String PASS_INPUT = "pass"
    private static String FAIL_INPUT = "fail"

    def setupSpec(){



    }


    def setup() {

        doc = JsonDocument.create("foo", new JsonObject().put("testData1", "xxxxx").put("testData2", "yyyyyy"))
        failDoc = JsonDocument.create("foo", new JsonObject().put("testData1", "zzzzz").put("testData2", "yyyyyy"))

        JsonObject jsonObject = JsonObject.create().put("test", doc.content())




        //Mock bucket and define dummy methods
        AsyncBucket mockBucket = Mock(AsyncBucket.class)


        //When name method is invoked on this object, then it returns "test".
        bucket.name() >> "test"

        Observable<JsonDocument> jsonObs = Observable.just(doc);

        Observable<JsonDocument> failObs = Observable.just(new Throwable());
    //    failObs.subscribe(_,_) >> {onNext,onError -> onError(new Throwable())  }

//When upsert method is called on this object with ANY parameter, it returns jsonObs object defined above
        bucket.upsert(_) >> jsonObs

        bucket.insert(doc) >> jsonObs
        bucket.remove(PASS_INPUT) >> jsonObs
        bucket.remove(doc) >> jsonObs
        bucket.get(doc) >> jsonObs
        bucket.get(PASS_INPUT) >> jsonObs
        bucket.replace(doc) >> jsonObs

        bucket.upsert(failDoc) >> failObs
        bucket.insert(failDoc) >> failObs
        bucket.remove(FAIL_INPUT) >> failObs
        bucket.remove(failDoc) >> failObs

        bucket.get(failDoc) >> failObs
        bucket.get(FAIL_INPUT) >> failObs

        bucket.replace(failDoc) >> failObs


        //Setup for mocking query method in bucket


        AsyncN1qlQueryRow asyncN1qlQueryRow = Mock()
        asyncN1qlQueryRow.value() >> jsonObject

        AsyncN1qlQueryResult asyncN1qlQueryResult = Mock()


        Observable<AsyncN1qlQueryResult> queryResultObs = Observable.just(asyncN1qlQueryResult);

        Observable<AsyncN1qlQueryRow> queryRowObs = Observable.just(asyncN1qlQueryRow);
        bucket.query(_) >> queryResultObs



        asyncN1qlQueryResult.rows() >> queryRowObs
asyncN1qlQueryResult.finalSuccess() >> Observable.just(true)



        CouchbaseAsyncCluster cluster = Mock()

        // AsyncCluster test = CouchbaseAsyncCluster.create("Testy")
        //  println test

        AsyncCluster asyncCluster = Mock(AsyncCluster.class)
        asyncCluster.authenticate(_,_) >> asyncCluster
        asyncCluster.openBucket(_) >> Observable.just(mockBucket)


new MockUp<CouchbaseAsyncCluster>(){

    public AsyncCluster create(String host){
        return asyncCluster;
    }

};
        io.vertx.core.json.JsonObject config = new io.vertx.core.json.JsonObject()
        config.put("hostName" , "test")
        config.put("username" , "x")
        config.put("password" , "x")
        config.put("bucketName" , "x")

        wrapper.getConnection(config,   { x-> if(x.succeeded()) { bucket = x.result()} })





        asyncConditions = new AsyncConditions()

    }


    def "creating a document by N1QL query"() {

        given:

        when:


        wrapper.createDocumentByN1QLQuery(bucket, N1qlQuery.simple("query"), {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            // print async.result().toString()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }


    def "creating a document"() {

        given:

        when:

        wrapper.createDocument(bucket, doc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()

                }
        })


        then:
        asyncConditions.await(1)


    }


    def "creating a document fail case"() {

        given:

        when:

        wrapper.createDocument(bucket, failDoc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()

                }
        })


        then:
        asyncConditions.await(1)


    }

    def "upserting a document"() {

        given:

        when:

        wrapper.upsertDocument(bucket, doc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //print async.result()
                            async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }

    def "upserting a document fail case"() {

        given:

        when:

        wrapper.upsertDocument(bucket, failDoc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //print async.result()
                            async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }

    def "getting a document"() {

        given:

        when:

        wrapper.getDocumentByKey(bucket, PASS_INPUT, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            // print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }


    def "getting a document fail"() {

        given:

        when:

        wrapper.getDocumentByKey(bucket, FAIL_INPUT, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            // print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }

    def "updating a document"() {

        given:

        when:

        wrapper.updateDocument(bucket, doc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }

    def "updating a document fail"() {

        given:

        when:

        wrapper.updateDocument(bucket, failDoc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }

    def "deleting a document"() {

        given:

        when:

        wrapper.deleteDocument(bucket, doc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }


    def "deleting a document fail"() {

        given:

        when:

        wrapper.deleteDocument(bucket, failDoc, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }


    def "deleting a document by key"() {

        given:

        when:

        wrapper.deleteDocumentByKey(bucket, PASS_INPUT, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //  print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }



    def "deleting a document by key fail"() {

        given:

        when:

        wrapper.deleteDocumentByKey(bucket, FAIL_INPUT, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //  print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }


    def "deleting a document by N1ql query"() {

        given:

        when:

        wrapper.deleteDocumentByN1QLQuery(bucket, N1qlQuery.simple("query"), {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //  print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }




    def "get all documents by N1ql query"() {

        given:

        when:

        wrapper.getAllDocuments(bucket, N1qlQuery.simple("query"), {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //  print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }



    def "updating a document by N1ql query"() {

        given:

        when:

        wrapper.updateDocumentByN1QLQuery(bucket, N1qlQuery.simple("query"), {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            //  print async.result()
                            assert async.result() != null
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }




}
