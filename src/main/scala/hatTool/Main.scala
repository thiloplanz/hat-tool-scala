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

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ning.http.client.AsyncHttpClient
import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Main {

  private val config = ConfigFactory.load()

  private val host = config.getString("hat.hostUrl")

  private val owner = config.getString("hat.owner.username")
  private val ownerPassword = config.getString("hat.owner.password")


  private val mapper =  new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT);


  def dumpJson(json: Future[Any], selector: Option[JsonPointer] = None) = try {
    val data = Await.result(json, Duration("10 seconds"))
    println(mapper.writeValueAsString(
      selector match {
        case None => data
        case Some(ptr) =>  mapper.valueToTree(data).asInstanceOf[JsonNode].at(ptr)
      }))
  } catch {
    case e: UnsuccessfulRequestException => println(e.toString)
  }

  def checkJsonPointer(expression: Option[String]) = expression.map { JsonPointer.valueOf(_) }

  def main(args: Array[String]) {

    lazy val ning = new AsyncHttpClient()
    lazy val client = HatClient.forOwner(new NingJsonClient(ning), host, owner, ownerPassword)




    object Args extends ScallopConf(args){
      val listCommands = new Subcommand("list") {

        val listDataSources = new Subcommand("sources") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() = checkJsonPointer(filter.get) match { case selector => dumpJson(client.listDataSources, selector) }
        }
        val listPersons = new Subcommand("persons") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() = checkJsonPointer(filter.get) match { case selector => dumpJson(client.listPersons, selector) }
        }
        val listThings = new Subcommand("things") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() = checkJsonPointer(filter.get) match { case selector => dumpJson(client.listThings, selector) }
        }
        val listOrgs = new Subcommand("organizations") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector => dumpJson(client.listOrganizations, selector) }
        }
        val listLocations = new Subcommand("locations") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector =>dumpJson(client.listLocations, selector) }
        }
        val listEvents = new Subcommand("events") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector => dumpJson(client.listEvents, selector) }
        }
        val listProperties = new Subcommand("properties") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector => dumpJson(client.listProperties, selector) }
        }
        val listPropertyTypes = new Subcommand("propertyTypes") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector => dumpJson(client.listPropertyTypes, selector) }
        }
        val listUnitsOfMeasurements = new Subcommand("units") with Runnable{
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector => dumpJson(client.listUnitsOfMeasurement, selector) }
        }

      }

      val describeCommands = new Subcommand("describe") {

        val describeDataTable = new Subcommand("dataTable") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.describeDataTable(id()), selector)
          }
        }
        val describeProperty = new Subcommand("property") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector =>
              scala.util.control.Exception.allCatch.opt(id().toInt) match {
                case None => dumpJson(client.describeProperty(id()), selector)
                case Some(number) => dumpJson(client.describeProperty(number), selector)
              }
          }
        }
        val describePropertyType = new Subcommand("propertyType") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector =>
              scala.util.control.Exception.allCatch.opt(id().toInt) match {
                case None => dumpJson(client.describePropertyType(id()), selector)
                case Some(number) => dumpJson(client.describePropertyType(number), selector)
              }
          }
        }
        val describeUnit = new Subcommand("unit") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector =>
              scala.util.control.Exception.allCatch.opt(id().toInt) match {
                case None => dumpJson(client.describeUnitOfMeasurement(id()), selector)
                case Some(number) => dumpJson(client.describeUnitOfMeasurement(number), selector)
              }
          }
        }
      }
      val dumpCommands = new Subcommand("dump") {

        val dumpDataTable = new Subcommand("dataTable") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.dumpDataTable(id()), selector)
          }
        }
        val dumpPerson = new Subcommand("person") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.getPerson(id()), selector)
          }
        }
        val dumpEvent = new Subcommand("event") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.getEvent(id()), selector)
          }
        }
        val dumpLocation = new Subcommand("location") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.getLocation(id()), selector)
          }
        }
        val dumpOrganization = new Subcommand("organization") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.getOrganization(id()), selector)
          }
        }
        val dumpThing = new Subcommand("thing") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.getThing(id()), selector)
          }
        }
        val dumpDataDebitValues = new Subcommand("dataDebitValues") with Runnable {
          val key = trailArg[String]()
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get) match { case selector => dumpJson(client.dumpDataDebitValues(key()), selector); }
        }
      }
      val createCommands = new Subcommand("create") {
        val createDataTable = new Subcommand("dataTable") with Runnable {
          val definition = trailArg[String]()

          override def run() = _createDataTable(client, definition())
        }
        val createBundle = new Subcommand("bundle") with Runnable {
          val table = opt[Int]()
          val name = trailArg[String]()

          override def run() = dumpJson(client.createContextlessBundle(name(), table()))
        }
      }
      val propose = new Subcommand("propose") {

        val proposeDataDebit = new Subcommand("dataDebit") with Runnable {
          val table = opt[Int]()
          val name = trailArg[String]()

          override def run() = dumpJson(client.proposeDataDebit(name(), new HatContextLessBundle(name(), table()),
            startDate = new Date()
          ))
        }
      }
      val enable = new Subcommand("enable") {
        val enableDataDebit = new Subcommand("dataDebit") with Runnable {
          val key = trailArg[String]()

          override def run() = dumpJson(client.enableDataDebit(key()));
        }
      }
      val disable = new Subcommand("disable") {
        val disableDataDebit = new Subcommand("dataDebit") with Runnable {
          val key = trailArg[String]()

          override def run() = dumpJson(client.disableDataDebit(key()));
        }
      }
      val rawPost = new Subcommand("POST") with Runnable {
        val path = trailArg[String]()
        val rawJson = trailArg[String]()
        override def run() = _rawPost(client, path(), rawJson())
      }

    }

    try {
      Args.subcommands.lastOption match {
        case Some(command: Runnable) => command.run()
        case Some(command: Subcommand) => command.printHelp()
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
