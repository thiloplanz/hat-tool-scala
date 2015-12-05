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

  private def get[T:Manifest](url: String, queryParams : Seq[(String, String)] = Seq.empty) =
    Await.result(client.get[T](url, queryParams = queryParams), timeout)

  "NingJsonClient"	>> {

    "can parse JSON" >>  { get[Map[String, JsonNode]](httpBin + "/get").get("args").get.toString must_== "{}" }

    "can send query parameters" >> { get[Map[String, JsonNode]](httpBin + "/get",
      queryParams = Seq("x" -> "y", "x" -> "z", "foo" -> "bar")).get("args").get.toString must_==
        """{"foo":"bar","x":["y","z"]}""" }

    "can return the raw Ning Response" >> { get[Response](httpBin + "/get").getStatusCode must_== 200 }

    "rejects non-200 status codes" >> {
      def bad = get[Integer](httpBin + "/status/400")

      bad must throwA[UnsuccessfulRequestException]
    }
  }


}
