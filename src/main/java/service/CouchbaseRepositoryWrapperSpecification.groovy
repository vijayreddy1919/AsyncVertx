package wrapperclass

import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.AsyncN1qlQueryResult
import com.couchbase.client.java.query.AsyncN1qlQueryRow
import com.couchbase.client.java.query.N1qlQuery
import rx.Observable
import rx.Subscriber
import rx.functions.Action1
import service.CouchbaseRepositoryWrapper
import service.MockCouchbaseClient
import service.Stuff
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions

class CouchbaseRepositoryWrapperSpecification extends Specification {


    private @Shared
    AsyncConditions asyncConditions
    private AsyncBucket bucket
    private CouchbaseRepositoryWrapper wrapper = new CouchbaseRepositoryWrapper();
    private JsonDocument doc;


    def setup() {

        doc = JsonDocument.create("foo", new JsonObject().put("testData1", "xxxxx").put("testData2", "yyyyyy"))
        JsonObject jsonObject = JsonObject.create().put("test", doc.content())
        AsyncN1qlQueryRow asyncN1qlQueryRow = Mock()
        asyncN1qlQueryRow.value() >> jsonObject

        AsyncN1qlQueryResult asyncN1qlQueryResult = Mock()


        //Mock bucket and define dummy methods
        bucket = Mock()
        bucket.name() >> { return "test" }

        Observable<JsonDocument> jsonObs = Observable.just(doc);


        bucket.upsert(_) >> jsonObs
        bucket.insert(_) >> jsonObs
        bucket.remove(_) >> jsonObs
        bucket.get(_) >> jsonObs
        bucket.replace(_) >> jsonObs



        //Setup for mocking query method in bucket

        Observable<AsyncN1qlQueryResult> queryResultObs = Observable.just(asyncN1qlQueryResult);

        Observable<AsyncN1qlQueryRow> queryRowObs = Observable.just(asyncN1qlQueryRow);
        bucket.query(_) >> queryResultObs



        asyncN1qlQueryResult.rows() >> { return queryRowObs }

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

    def "getting a document"() {

        given:

        when:

        wrapper.getDocumentByKey(bucket, "foo", {

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


    def "deleting a document by key"() {

        given:

        when:

        wrapper.deleteDocumentByKey(bucket, "foo", {

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
