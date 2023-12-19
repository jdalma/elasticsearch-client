package javaclient

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.GetRequest
import co.elastic.clients.elasticsearch.core.IndexRequest
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import co.elastic.clients.elasticsearch.core.bulk.UpdateAction
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

/**
 * 고수준 REST 클라이언트는 요청과 응답에 엘라스틱서치 내부 구현에 사용된 클래스를 사용해야 했다.
 * 하지만 새 자바 클라이언트는 이런 부분에 Jackson 같은 JSON 라이브러리를 사용해서 요청과 응답에 사용자가 제공하는 클래스를 사용하게 한다.
 *
 * 이를 통해 기존 고수준 REST 클라이언트보다 엘라스틱서치 서버의 내부 구현 사항과 결합도를 낮추고 클라이언트와 서버의 호환 이슈를 줄였다.
 * 내부적으로 저수준 REST 클라이언트를 사용한다.
 */

fun main() {
    val javaClient = buildJavaClient()
    // indexSample(javaClient)
    // getExample(javaClient)
    builderBulkExample(javaClient)
}

private fun buildJavaClient(): ElasticsearchClient {
    val lowLevelRestClient = RestClient.builder(
        HttpHost("localhost", 9200, "https")
    ).setRequestConfigCallback {
        it.setConnectTimeout(5000).setSocketTimeout(70000)
    }.build()

    val mapper = jacksonMapperBuilder()
        .addModule(JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .build()

    val transport = RestClientTransport(lowLevelRestClient, JacksonJsonpMapper(mapper))
    return ElasticsearchClient(transport) // 자바 클라이언트
}

private fun indexSample(client: ElasticsearchClient) {
    val indexResponse = client.index { builder: IndexRequest.Builder<MyIndex3Class>  ->
        builder.index("my_index3")
            .id("random")
            .document(
                MyIndex3Class(
                "client test1",
                    "client test2",
                    true,
                    "client test3",
                    "client test4",
                    10000
                )
            )
    }

    println(indexResponse.result())
}

private fun getExample(client: ElasticsearchClient) {
    val getRequest = GetRequest.Builder()
        .index("my_index3")
        .id("r3yzg4wB1VN7bxlubx69")
        .build()

    println(client.get(getRequest, MyIndex3Class::class.java))
}

private fun builderBulkExample(client: ElasticsearchClient) {
    val createOperation = CreateOperation.Builder<MyIndex3Class>()
        .index("my_index3")
        .document(MyIndex3Class(
            "client test5",
            "client test6",
            true,
            "client test7",
            "client test8",
            20000
        ))
        .build()

    val indexOperation = IndexOperation.Builder<MyIndex3Class>()
        .index("my_index3")
        .id("indexoperation")
        .document(MyIndex3Class(
            "client test9",
            "client test10",
            true,
            "client test11",
            "client test12",
            30000
        ))
        .build()

    val updateAction = UpdateAction.Builder<MyIndex3Class, MyIndex3PartialClass>()
        .doc(MyIndex3PartialClass(
            "update client test9",
            "update client test10"
        ))
        .build()

    val updateOperation = UpdateOperation.Builder<MyIndex3Class, MyIndex3PartialClass>()
        .index("my_index3")
        .action(updateAction)
        .build()

    val bulkOpOne = BulkOperation.Builder().create(createOperation).build()
    val bulkOpTwo = BulkOperation.Builder().index(indexOperation).build()
    val bulkOpThree = BulkOperation.Builder().update(updateOperation).build()

    val bulkResponse = client.bulk { it.operations(listOf(bulkOpOne, bulkOpTwo, bulkOpThree)) }
    bulkResponse.items().forEach {
        println("result : ${it.result()}, error : ${it.error()}")
    }
}

private fun functionalBulkExample(client: ElasticsearchClient) {
    val bulkResponse = client.bulk { requestBuilder -> requestBuilder
        .operations { operationBuilder -> operationBuilder
            .index { indexOperationBuilder: IndexOperation.Builder<MyIndex3Class> ->
                indexOperationBuilder
                    .index("my_index3")
                    .id("rnyTg4wB1VN7bxluKh5g")
                    .document(MyIndex3Class(
                        "client test13",
                        "client test14",
                        true,
                        "client test15",
                        "client test16",
                        40000
                    ))
            }
        }
        .operations { operationBuilder -> operationBuilder
            .update { updateOperationBuilder: UpdateOperation.Builder<MyIndex3Class, MyIndex3PartialClass> ->
                updateOperationBuilder
                    .index("my_index3")
                    .id("r3yzg4wB1VN7bxlubx69")
                    .action { updateActionBuilder ->
                        updateActionBuilder.doc(MyIndex3PartialClass("update field1!!!", "update field2!!!"))
                    }
            }
        }
    }

    bulkResponse.items().forEach {
        println("result : ${it.result()}, error : ${it.error()}")
    }
}

private fun searchExample(client: ElasticsearchClient) {
    val searchRequestBuilder = { builder: SearchRequest.Builder ->
        builder.index("my_index3")
            .from(0)
            .size(10)
            .query {
                it.term { term ->
                    term.field("field1")
                        .value { it.stringValue("world") }
                }
            }
    }
    val response = client.search(searchRequestBuilder, MyIndex3Class::class.java)

    response.hits().hits().forEach {
        println("soruce : ${it.source()}")
    }
}

private fun bulkIngesterExample(client: ElasticsearchClient) {
    val listener = BulkIngestListener<String>()

    val ingester = BulkIngester.of {
        it.client(client)
            .maxOperations(200)
            .maxConcurrentRequests(1)
            .maxSize(5242880L) // 5MB
            .flushInterval(5L, TimeUnit.SECONDS)
            .listener(listener)
    }

    for (number in 0L..<1100L) {
        val bulkOperation = BulkOperation.of { bulkBuilder ->
            bulkBuilder.index { indexOpBuilder: IndexOperation.Builder<MyIndex3Class> ->
                indexOpBuilder
                    .index("my_index3")
                    .id("my-id-$number")
                    .routing("my-routing-$number")
                    .document(
                        MyIndex3Class(
                            "client test-$number",
                            "client test-$number",
                            true,
                            "client test-$number",
                            "client test-$number",
                            number.toInt()
                        )
                    )
            }
        }
        ingester.add(bulkOperation, "my-context-$number")
    }

    println("[${LocalDateTime.now()}] sleep 10 seconds ...")
    Thread.sleep(10000L)

    for (number in 1100L until 1200L) {
        val bulkOperation = BulkOperation.of { bulkBuilder ->
            bulkBuilder.index { indexOpBuilder: IndexOperation.Builder<MyIndex3Class> ->
                indexOpBuilder
                    .index("my-index")
                    .id("my-id-$number")
                    .routing("my-routing-$number")
                    .document(
                        MyIndex3Class(
                            "client test-$number",
                            "client test-$number",
                            true,
                            "client test-$number",
                            "client test-$number",
                            number.toInt()
                        )
                    )
            }
        }

        ingester.add(bulkOperation, "my-context-$number")
    }

    println("[${LocalDateTime.now()}] sleep 10 seconds ...")
    Thread.sleep(10000L)

    ingester.close()
}
