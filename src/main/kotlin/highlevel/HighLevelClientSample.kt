package highlevel

import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.message.BasicHeader
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.*
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.core.TimeValue
import org.elasticsearch.index.query.*
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.xcontent.XContentType

/**
 * 고수준 REST 클라이언트는 엘라스틱 서치 API를 클라이언트 라이브러리의 API로 노출한다.
 * 하지만 엘라스틱서치에 강결합되어 있어 버진 이슈가 있을 수 있다.
 * 메이저 버전을 업그레이드할 때는 클라이언트의 버전도 함께 맞춰 올려주는 것이 좋다.
 * 하지만 7.15부터 지원 중단 되었고, 새 자바 클라이언트가 등장했다.
 * 5.0 부터 7.15 버전 사이의 엘라스틱서치를 사용한다면 고수준 REST 클라이언트를 선택하면 된다.
 */

fun main() {
    val highLevelClient = RestHighLevelClientBuilder(buildClient())
        .setApiCompatibilityMode(true)
        .build()
    println("<getSample>")
    getSample(highLevelClient)
    println("</getSample>")

    println("<searchSample>")
    searchSample(highLevelClient)
    println("</searchSample>")

    println("<bulkSample>")
    bulkSample(highLevelClient)
    println("</bulkSample>")

    println("<bulkProcessorSample>")
    val buildBulkProcessor = buildBulkProcessor(highLevelClient)
    bulkProcessorSample(buildBulkProcessor)
    println("</bulkProcessorSample>")

    buildBulkProcessor.close()
    highLevelClient.close()
}

private fun buildClient(): RestClient = RestClient.builder(
        HttpHost("localhost", 9200, "http")
    ).setDefaultHeaders(
        arrayOf<Header>(BasicHeader("my-header", "my-value"))
    ).setRequestConfigCallback {
        it.setConnectTimeout(5000).setSocketTimeout(70000)
    }.build()

private fun getSample(highLevelClient: RestHighLevelClient) {
    val index = GetRequest().index("my_index3").id("rHyRg4wB1VN7bxluqh75")
    val getResponse = highLevelClient.get(index, RequestOptions.DEFAULT)
    println(getResponse.sourceAsMap)
}

private fun searchSample(highLevelClient: RestHighLevelClient) {
    // QueryBuilders 하위의 정적 메소드에 주요 검색 쿼리 생성을 위한 API들이 모여있다.
    val queryBuilder = QueryBuilders.boolQuery()
        .must(TermQueryBuilder("field1", "hello"))
        .should(MatchQueryBuilder("field2", "world").operator(Operator.AND))
        .should(RangeQueryBuilder("field6").gte(0).lt(100))
        .minimumShouldMatch(1)

    // 검색 API의 from, size, query, aggs 등을 지정한다.
    val searchSourceBuilder = SearchSourceBuilder()
        .from(0)
        .size(10)
        .query(queryBuilder)

    // 검색 대상, 인덱스, 라우딩 등을 지정하고 검색의 요청 본문을 삽입한다.
    val searchRequest = SearchRequest()
        .indices("my_index3")
        .source(searchSourceBuilder)

    val response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT)
    println(response)
    println(response.hits)
    println(response.hits.totalHits)
    println(response.hits.map { it.sourceAsMap })
}

/**
 * BulkRequest를 만들어서 IndexRequest, UpdateRequest, DeleteRequest를 원하는 만큼 추가한 뒤 bulk나 bulkAsync 메서드를 호출해서 사용한다.
 */
private fun bulkSample(highLevelClient: RestHighLevelClient) {
    val bulkRequest = BulkRequest().add(
        UpdateRequest()
            .index("my_index3")
            .id("rHyRg4wB1VN7bxluqh75")
            .doc(mapOf("field1" to "updated field!!!"))
    )

    val bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT)
    if (bulkResponse.hasFailures()) {
        println(bulkResponse.buildFailureMessage())
    }

    val index = GetRequest().index("my_index3").id("rHyRg4wB1VN7bxluqh75")
    val getResponse = highLevelClient.get(index, RequestOptions.DEFAULT)
    println(getResponse.sourceAsMap)
}

/**
 * Processor를 생성해두고 하위 요청을 원하는 만큼 add해 두면 BulkProcessor가 알아서 지정된 기준에 맞춰 bulk 요청을 만들어 보낸다.
 * 이렇게 쌓여 있는 하위 요청을 bulk 요청으로 만들어서 보내는 것을 flush 한다고 표현한다.
 * - setBulkActions : 하위 요청이 몇 개 모이면 flush를 수행할지 지정한다.
 * - setBulkSize : 쌓인 하위 요청의 크기가 얼마를 초과헀을때 flush를 수행할지 지정한다.
 * - setFlushInterval : bulk 요청을 어떤 주기로 보낼지 지정한다. 보내야 하는 bulk 요청의 개수가 한참 많이 남아 있는 상황이더라도 이 주기를 지켜서 요청한다.
 * 반대로 쌓여 있는 하위 요청이 몇 개 없더라도 이 주기가 지나면 flush를 수행한다.
 * - setConcurrentRequests : 동시에 몇 개까지 bulk 요청을 수행할지 지정한다. bulk 요청이 날아가고 난 뒤 응답이 돌아올 때까지 대기중인 작업을 최대 몇 개까지 허용하는지 지정한다.
 * - setBackoffPolicy : 스레드 풀 부족 등 자원 문제로 bulk 작업이 실패했을 때 BulkProcessor가 어떤 정책으로 재시도를 수행할지 지정한다.
 *
 * 단일 bulk 요청 내부의 하위 요청 중 완전히 동일한 인덱스, _id, 라우팅 조합을 가진 요청은 그 bulk 요청에 기술된 순서대로 동작한다고 배웠다.
 * 즉, 작업 순서의 역전이 일어나지 않는다.
 * 그러나 여러 bulk 요청이 동시에 들어갈 때 동일한 문서를 대상으로 한 하위 요청이 각기 다른 bulk 요청으로 들어갔다면 작업 순서의 역전이 일어날 수 있다.
 * 이를 회피하기 위해서는 setConcurrentRequests에 1을 지정해야 한다.
 * 작업 순서의 역전을 신경쓰지 않아도 되는 상황에는 값을 높여보면서 성능을 조절할 수 있다.
 */
private fun buildBulkProcessor(highLevelClient: RestHighLevelClient): BulkProcessor {
    val bulkAsync = { request: BulkRequest , listener: ActionListener<BulkResponse> ->
        highLevelClient.bulkAsync(request, RequestOptions.DEFAULT, listener)
        Unit
    }

    return BulkProcessor.builder(bulkAsync, EsBulkListener() , "myBulkProcessor")
        .setBulkActions(50000)
        .setBulkSize(ByteSizeValue.ofMb(50L))
        .setFlushInterval(TimeValue.timeValueMillis(5000L))
        .setConcurrentRequests(1)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff())
        .build()
}

fun bulkProcessorSample(buildBulkProcessor: BulkProcessor) {
    val source = mapOf(
        "hello" to "world!!!",
        "world" to "hello!!!"
    )

    val indexRequest = IndexRequest("my_index3")
        .id("sHyzg4wB1VN7bxluoB4n")
        .source(source, XContentType.JSON)

    buildBulkProcessor.add(indexRequest)
}
