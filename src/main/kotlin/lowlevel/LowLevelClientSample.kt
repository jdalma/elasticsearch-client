package lowlevel

import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Cancellable
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import java.lang.Exception
import kotlin.text.Charsets.UTF_8

fun main() {
    val client = buildClient()
    request(client)
    asyncRequest(client)

    client.close()
}

private fun buildClient(): RestClient = RestClient.builder(
        HttpHost("localhost", 9200, "http")
    ).setDefaultHeaders(
        arrayOf<Header>(BasicHeader("my-header", "my-value"))
    ).setRequestConfigCallback {
        it.setConnectTimeout(5000).setSocketTimeout(70000)
    }.build()

private fun request(client: RestClient) {
    // sync request
    val getSettingRequest = Request("GET", "/my_index2/_search").apply {
        addParameters(mapOf("pretty" to "true"))
    }

    val response: Response = client.performRequest(getSettingRequest)
    printResponse(response)
}

private fun asyncRequest(client: RestClient) {
    // async request
    val updateSettingsRequest = Request("POST", "/my_index3/_doc")
    val requestBody = """
        {
          "field1": "hello",
          "field2": "world",
          "field3": true,
          "field4": "elasticsearch",
          "field5": "lucene"
        }
    """.trimIndent()
    updateSettingsRequest.entity = NStringEntity(requestBody, ContentType.APPLICATION_JSON)

    val responseListener = object : ResponseListener {
        override fun onSuccess(response: Response) {
            println("onSuccess : $response")
        }

        override fun onFailure(exception: Exception) {
            println("onFailure : $exception")
        }
    }

    val cancellable: Cancellable = client.performRequestAsync(updateSettingsRequest, responseListener)

    Thread.sleep(3000L)
}

private fun printResponse(response: Response) {
    val statusCode = response.statusLine.statusCode
    val responseBody = EntityUtils.toString(response.entity, UTF_8)
    println("statusCode : $statusCode , responseBody : $responseBody")
}
