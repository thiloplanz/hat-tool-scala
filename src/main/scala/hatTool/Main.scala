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


package hatTool

import com.fasterxml.jackson.databind.{SerializationFeature, ObjectMapper}
import com.ning.http.client.AsyncHttpClient
import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  private val config = ConfigFactory.load()

  private val host = config.getString("hat.hostUrl")

  private val owner = config.getString("hat.owner.username")
  private val ownerPassword = config.getString("hat.owner.password")



  def main(args: Array[String]) {

    lazy val ning = new AsyncHttpClient()
    lazy val client = HatClient.forOwner(new NingJsonClient(ning), host, owner, ownerPassword)

    val mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);

    object Args extends ScallopConf(args){
      val listDataSources = new Subcommand("listDataSources") with Runnable{
        val dummy = opt[String]()  // weird Scallop parser workaround
        override def run() = println(mapper.writeValueAsString(
            Await.result(client.listDataSources(), Duration("10 seconds"))))
      }
    }

    try {
      Args.subcommand match {
        case Some(command: Runnable) => command.run()
        case _ => Args.printHelp()
      }
    }
    finally{
      ning.close()
    }

  }

}
