package service

import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.N1qlQuery
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions

class CouchbaseRepositoryWrapperSpecification extends Specification {


    private @Shared AsyncConditions asyncConditions
    private @Shared AsyncBucket bucket
    private CouchbaseRepositoryWrapper wrapper = new CouchbaseRepositoryWrapper();
    private JsonDocument doc1;
    private JsonDocument doc2;
    private JsonDocument doc3;

    private @Shared MockCouchbaseClient mockCouchbaseClient = MockCouchbaseClient.getInstance();



    def setupSpec(){

        asyncConditions = new AsyncConditions()


        mockCouchbaseClient.createClient({

            async -> bucket = async; asyncConditions.evaluate { bucket != null }
        }
        );

        asyncConditions.await(5);

    }

    def setup() {

        doc1 = JsonDocument.create("foo", new JsonObject().put("testData1", "xxxxx").put("testData2", "yyyyyy"))
        doc2 = JsonDocument.create("moo", new JsonObject().put("testData1", "aaaaa").put("testData2", "yyyyyy"))
        doc3 = JsonDocument.create("moo", new JsonObject().put("testData1", "aaaaa").put("testData2", "zzzzzz"))

        asyncConditions = new AsyncConditions()
    }






    def "creating a document by N1QL query"() {

        given:

        when:



        wrapper.createDocumentByN1QLQuery(bucket, N1qlQuery.simple("INSERT INTO `travel-sample` ( KEY, VALUE ) \n" +
            "  VALUES \n" +
            "  ( \n" +
            "    \"k001\", \n" +
            "    { \"id\": \"01\", \"type\": \"airline\"} \n" +
            "  ) "), {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result().toString()
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

        wrapper.createDocument(bucket, doc1, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
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

        wrapper.upsertDocument(bucket, doc2, {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
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
                            print async.result()
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

        wrapper.updateDocument(bucket, doc2 , {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
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

        wrapper.deleteDocument(bucket, doc3 , {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
                        }
                }


                if (async.failed()) {
                    print async.cause()
                }
        })


        then:
        asyncConditions.await(1)


    }


    def "updating a document by key"() {

        given:

        when:

        wrapper.deleteDocumentByKey(bucket, "foo" , {

            async ->
                if (async.succeeded()) {
                    asyncConditions.evaluate
                        {
                            print async.result()
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
