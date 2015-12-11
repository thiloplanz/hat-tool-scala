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

import java.io.File
import java.util.Date

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ning.http.client.AsyncHttpClient
import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Main {

  private val config = ConfigFactory.load()

  private val host = config.getString("hat.hostUrl")

  private val owner = config.getString("hat.owner.username")
  private val ownerPassword = config.getString("hat.owner.password")


  private val mapper =  new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT);


  def dumpJson(json: Future[Any]) = try {
    println(mapper.writeValueAsString(Await.result(json, Duration("10 seconds"))))
  } catch {
    case e: UnsuccessfulRequestException => println(e.toString)
  }

  def main(args: Array[String]) {

    lazy val ning = new AsyncHttpClient()
    lazy val client = HatClient.forOwner(new NingJsonClient(ning), host, owner, ownerPassword)


    object Args extends ScallopConf(args){
      val listDataSources = new Subcommand("listDataSources") with Runnable{
        val dummy = opt[String]()  // weird Scallop parser workaround
        override def run() = dumpJson(client.listDataSources)
      }
      val describeDataTable = new Subcommand("describeDataTable") with Runnable{
        val id = trailArg[Int]()
        override def run() = dumpJson(client.describeDataTable(id()))
      }
      val dumpDataTable = new Subcommand("dumpDataTable") with Runnable{
        val id = trailArg[Int]()
        override def run() = dumpJson(client.dumpDataTable(id()))
      }
      val createDataTable = new Subcommand("createDataTable") with Runnable {
        val definition = trailArg[String]()
        override def run() = _createDataTable(client, definition())
      }
      val createBundle = new Subcommand("createBundle") with Runnable {
        val table = opt[Int]()
        val name = trailArg[String]()
        override def run() =  dumpJson(client.createContextlessBundle(name(), table()))
      }
      val proposeDataDebit = new Subcommand("proposeDataDebit") with Runnable {
        val table = opt[Int]()
        val name = trailArg[String]()

        override def run() = dumpJson(client.proposeDataDebit(name(), new HatContextLessBundle(name(), table()),
          startDate = new Date()
        ))
      }
      val enableDataDebit = new Subcommand("enableDataDebit") with Runnable {
        val key = trailArg[String]()
        override def run() = dumpJson(client.enableDataDebit(key()));
      }
      val disableDataDebit = new Subcommand("disableDataDebit") with Runnable {
        val key = trailArg[String]()
        override def run() = dumpJson(client.disableDataDebit(key()));
      }
      val dumpDataDebitValues = new Subcommand("dumpDataDebitValues") with Runnable {
        val key = trailArg[String]()
        override def run() = dumpJson(client.dumpDataDebitValues(key()));
      }
      val rawPost = new Subcommand("POST") with Runnable {
        val path = trailArg[String]()
        val rawJson = trailArg[String]()
        override def run() = _rawPost(client, path(), rawJson())
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

  private def _createDataTable(client: HatClient, fileName: String): Unit ={
    val definition = mapper.readValue[ObjectNode](new File(fileName))
    dumpJson(client.createDataTable(definition))
  }

  private def _rawPost(client: HatClient, path: String, fileName: String): Unit = {
    val entity = mapper.readValue[ObjectNode](new File(fileName))
    dumpJson(client.rawPost(path, entity))
  }

}
