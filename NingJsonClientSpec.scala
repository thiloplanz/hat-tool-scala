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

import com.fasterxml.jackson.databind.JsonNode
import com.ning.http.client.{AsyncHttpClient, Response}
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration.Duration


class NingJsonClientSpec extends Specification{

  val httpBin = "http://httpbin.org"

  val timeout = Duration("10 seconds")

  val ning = new AsyncHttpClient()

  val client = new NingJsonClient(ning)

  type HttpBinResponse = Map[String, JsonNode]

  private def get[T:Manifest](url: String, queryParams : Seq[(String, String)] = Seq.empty) =
    Await.result(client.get[T](httpBin + url, queryParams = queryParams), timeout)

  private def post[T:Manifest](url: String, entity: Any, queryParams : Seq[(String, String)] = Seq.empty) =
    Await.result(client.postJson[T](httpBin + url, entity, queryParams = queryParams), timeout)

  private def postText[T:Manifest](url: String, entity: String, queryParams : Seq[(String, String)] = Seq.empty) =
    Await.result(client.postText[T](httpBin + url, entity, queryParams = queryParams), timeout)

  private def postBytes[T:Manifest](url: String, entity: Array[Byte], queryParams : Seq[(String, String)] = Seq.empty) =
    Await.result(client.postBytes[T](httpBin + url, entity, queryParams = queryParams), timeout)


  "NingJsonClient"	>> {

    "can parse JSON" >>  { get[HttpBinResponse]("/get").get("args").get.toString must_== "{}" }

    "can post JSON" >> { post[HttpBinResponse]("/post", Map("hey" -> "ho")).get("json").get.get("hey").asText must_== "ho" }

    "can post text" >> { postText[HttpBinResponse]("/post", "super'dooper").get("data").get.asText  must_== "super'dooper" }

    "can post bytes" >> { postBytes[HttpBinResponse]("/post", Array[Byte]('a','b','c')).get("data").get.asText must_== "abc" }


    "can send query parameters" >> { get[HttpBinResponse]("/get",
      queryParams = Seq("x" -> "y", "x" -> "z", "foo" -> "bar")).get("args").get.toString must_==
        """{"foo":"bar","x":["y","z"]}""" }

    "can return the raw Ning Response" >> { get[Response]( "/get").getStatusCode must_== 200 }

    "rejects non-200 status codes" >> {
      def bad = get[Integer]("/status/400")

      bad must throwA[UnsuccessfulRequestException]
    }
  }


}
