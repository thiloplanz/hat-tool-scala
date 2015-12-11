// Copyright (c) 2015, Thilo Planz.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the Apache License, Version 2.0
// as published by the Apache Software Foundation (the "License").
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// You should have received a copy of the License along with this program.
// If not, see <http://www.apache.org/licenses/LICENSE-2.0>.

import java.io.{InputStream, IOException}
import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ning.http.client._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Future, Promise}

/**
 * Combines the Ning HTTP client library and the Jackson JSON library.
 *
 * Tries to make the following scenario trivial:
 *
 * 1) send HTTP request with optional application/json entity
 * 2) check for "200 OK" result status
 * 3) if OK, receive application/json result
 * 4) JSON (in and out) mapped to Scala classes of your choice without much hassle
 *
 * Also allows variations from that scenario with a reasonable amount of configuration.
 *
 * - posting something other than JSON
 * - "unusual" headers
 * - retrieving something other than JSON
 * - different error handling (default just errors out)
 * - supplying your custom version of Ning and Jackson ObjectMapper
 *
 *
 */

class NingJsonClient(ning: AsyncHttpClient,
                     objectMapper: ObjectMapper with ScalaObjectMapper = NingJsonClient.defaultObjectMapper,
                     logger : Logger = NingJsonClient.logger) {

  def get[T: Manifest](url: String, 
                       queryParams: Seq[(String, String)] = Map.empty.toList, 
                       requestHeaders : RequestHeaders = RequestHeaders.AcceptJson, 
                       okayStatusCode: Int = 200
           ) : Future[T] = executeRequest[T](ning.prepareGet(url), url, queryParams, requestHeaders, okayStatusCode)

  def postJson[T: Manifest](url: String,
                            entity: Any,
                            queryParams: Seq[(String, String)] = Map.empty.toList,
                            requestHeaders : RequestHeaders = RequestHeaders.SendAndAcceptJson,
                            okayStatusCode: Int = 200
                             ): Future[T] =
    postBytes(url, objectMapper.writeValueAsBytes(entity), queryParams, requestHeaders, okayStatusCode)

  def postText[T: Manifest](url: String,
                            entity: String,
                            queryParams: Seq[(String, String)] = Map.empty.toList,
                            requestHeaders : RequestHeaders = RequestHeaders.SendTextAcceptJson,
                            okayStatusCode: Int = 200
                             ): Future[T] =
    postBytes[T](url, entity.getBytes(NingJsonClient.UTF8), queryParams, requestHeaders, okayStatusCode)


  def postBytes[T: Manifest](url: String,
                            entity: Array[Byte],
                            queryParams: Seq[(String, String)] = Map.empty.toList,
                            requestHeaders : RequestHeaders = RequestHeaders.SendOctetsAcceptJson,
                            okayStatusCode: Int = 200
                             ): Future[T] =
    executeRequest[T](ning.preparePost(url).setBody(entity), url, queryParams, requestHeaders, okayStatusCode)

  def postByteStream[T: Manifest](url: String,
                                  entity: InputStream, 
                                  queryParams: Seq[(String, String)] = Map.empty.toList, 
                                  requestHeaders : RequestHeaders = RequestHeaders.SendOctetsAcceptJson, 
                                  okayStatusCode: Int = 200
                              ): Future[T] =
    executeRequest[T](ning.preparePost(url).setBody(entity), url, queryParams, requestHeaders, okayStatusCode)

  def putJson[T: Manifest](url: String,
                            entity: Any,
                            queryParams: Seq[(String, String)] = Map.empty.toList,
                            requestHeaders : RequestHeaders = RequestHeaders.SendAndAcceptJson,
                            okayStatusCode: Int = 200
                             ): Future[T] =
    putBytes(url, objectMapper.writeValueAsBytes(entity), queryParams, requestHeaders, okayStatusCode)

  def putText[T: Manifest](url: String,
                            entity: String,
                            queryParams: Seq[(String, String)] = Map.empty.toList,
                            requestHeaders : RequestHeaders = RequestHeaders.SendTextAcceptJson,
                            okayStatusCode: Int = 200
                             ): Future[T] =
    putBytes[T](url, entity.getBytes(NingJsonClient.UTF8), queryParams, requestHeaders, okayStatusCode)


  def putBytes[T: Manifest](url: String,
                             entity: Array[Byte],
                             queryParams: Seq[(String, String)] = Map.empty.toList,
                             requestHeaders : RequestHeaders = RequestHeaders.SendOctetsAcceptJson,
                             okayStatusCode: Int = 200
                              ): Future[T] =
    executeRequest[T](ning.preparePut(url).setBody(entity), url, queryParams, requestHeaders, okayStatusCode)

  def putByteStream[T: Manifest](url: String,
                                  entity: InputStream,
                                  queryParams: Seq[(String, String)] = Map.empty.toList,
                                  requestHeaders : RequestHeaders = RequestHeaders.SendOctetsAcceptJson,
                                  okayStatusCode: Int = 200
                                   ): Future[T] =
    executeRequest[T](ning.preparePut(url).setBody(entity), url, queryParams, requestHeaders, okayStatusCode)



  private def executeRequest[T: Manifest](request: RequestBuilderBase[_],
                                          url: String, queryParams: Seq[(String, String)],
                                          requestHeaders : RequestHeaders,
                                          okayStatusCode: Int) = {
    queryParams.foreach { case (name, value) => request.addQueryParam(name, value) }
    requestHeaders.headers.foreach { case (name, value) => request.addHeader(name, value)}

    val result = Promise[T]()
    ning.executeRequest(request.build,
      new AsyncCompletionHandler[Unit] {
        override def onCompleted(response: Response): Unit = {
          // do they want the raw Response ?
          if (classOf[Response].isAssignableFrom(manifest[T].runtimeClass)){
            result.success(response.asInstanceOf[T])
            return
          }
          // otherwise we require the expected status code (default: 200 OK)
          val status = HttpStatus(response.getStatusCode, response.getStatusText, url, queryParams)
          if (status.statusCode != okayStatusCode){
            result.failure(new UnsuccessfulRequestException(status))
            return
          }
          // and we expect the body to be JSON
          val entity = response.getResponseBodyAsBytes
          result.success(objectMapper.readValue(entity)(manifest[T]))

        }
        override def onThrowable(t: Throwable) = t match {
          case io: IOException =>
            logger.warn("IO error when calling "+url+": " + io.toString);
            result.failure(new UnsuccessfulRequestException(HttpStatus(0, t.toString, url, queryParams)))
          case _ => logger.error("crashed when calling " + url, t); result.failure(t)
        }

      }
    )

    result.future
  }


}

object NingJsonClient {

  private val defaultObjectMapper = new ObjectMapper with ScalaObjectMapper
  defaultObjectMapper.registerModule(DefaultScalaModule)
  defaultObjectMapper.registerModule(new JavaTimeModule)

  private val logger = LoggerFactory.getLogger("NingJsonClient")

  private val UTF8 = Charset.forName("UTF-8")

}


/**
 * Helper to build HTTP request headers.
 */

class RequestHeaders private(val headers: Map[String, String]) {

  def withHeader(name: String, value: String) = new RequestHeaders(this.headers + (name -> value))

  def withAccept(contentType: String) = withHeader("Accept", contentType)

  def withContentType(contentType: String) = withHeader("Content-Type", contentType)

}

object RequestHeaders {

  val None = new RequestHeaders(Map.empty)

  val AcceptJson = None.withAccept("application/json")

  val SendAndAcceptJson = AcceptJson.withContentType("application/json")

  val SendTextAcceptJson = AcceptJson.withContentType("text/plain; charset=UTF-8")

  val SendOctetsAcceptJson = AcceptJson.withContentType("application/octet-stream")

}

case class HttpStatus(statusCode: Int, statusText: String, requestUrl: String, requestQueryParams: Seq[(String, String)])

class UnsuccessfulRequestException(val status: HttpStatus) extends RuntimeException(
  "Request to "+status.requestUrl+" returned status code "+status.statusCode+" "+status.statusText)