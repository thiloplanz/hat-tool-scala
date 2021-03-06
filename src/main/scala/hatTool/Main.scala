// Copyright (c) 2015/2016, Thilo Planz.
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
import com.ning.http.client.{AsyncHttpClientConfig, AsyncHttpClient}
import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Main {

  private val config = ConfigFactory.load()

  private val host = config.getString("hat.hostUrl")
  private val sslCert = if (config.hasPath("hat.sslCert")) Some(config.getString("hat.sslCert")) else None

  private val owner = config.getString("hat.owner.username")
  private val ownerPassword = config.getString("hat.owner.password")

  private val timelimit = Duration("10 seconds")

  private val mapper =  new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT);


  def dumpJson(json: Future[Any], selector: Option[JsonPointer] = None) = try {
    val data = Await.result(json, timelimit)
    println(mapper.writeValueAsString(
      selector match {
        case None => data
        case Some(ptr) =>  mapper.valueToTree(data).asInstanceOf[JsonNode].at(ptr)
      }))
  } catch {
    case e: UnsuccessfulRequestException => println(e.toString)
  }

  def dumpGrid(full: Option[Boolean], json: Future[Seq[ObjectNode]], columns: Seq[String], selector: Option[JsonPointer]) = full match {
    case Some(true) => dumpJson(json, selector)
    case _ => {
      try {
        val data = Await.result(json, timelimit).map { row =>
          columns.map { col =>
            row.at("/"+col).asText()
          }
        }
        println(Tabulator.format(Seq(columns) ++ data))

      } catch {
        case e: UnsuccessfulRequestException => println(e.toString)
      }
    }
  }


  def checkJsonPointer(expression: Option[String]) : Option[JsonPointer] = expression.map { JsonPointer.valueOf(_) }

  def checkJsonPointer(expression: Option[String], grid: Option[Boolean], full: Option[Boolean]) : Option[JsonPointer] =
    (grid, full) match {
      case (Some(true), Some(true)) => throw new IllegalArgumentException("specify either --grid or --full, but not both")
      case (Some(false), Some(false)) => throw new IllegalArgumentException("specify either --grid or --full")
      case (_, Some(true)) => checkJsonPointer(expression)
      case _ => if (expression == None) None else throw new IllegalArgumentException("cannot use --grid with a JsonPointer expression ('"+expression.get+"')")
    }


  def main(args: Array[String]) {

    lazy val ning  = sslCert match {
      case None => new AsyncHttpClient
      case Some(cert) => new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
        .setSSLContext(SSLKeyPinning(cert).makeSSLContext(skipX509ExtendedTrustVerification = true))
        .build)
    }

    object Args extends ScallopConf(args){
      val accessToken = opt[String]("accessToken")

      lazy val client = accessToken.get match {
          case None => HatClient.forOwner(new NingJsonClient(ning), host, owner, ownerPassword)
          case Some(token) => HatClient.forAccessToken(new NingJsonClient(ning), host, token)
        }

      lazy val dataDict = new HatDataDictionaryCache(client)

      val listCommands = new Subcommand("list") {

        val listDataSources = new Subcommand("sources") with Runnable{
          val grid = toggle()
          val full = toggle()
          val filter = trailArg[String](required = false)
          override def run() = checkJsonPointer(filter.get, grid.get, full.get) match { case selector =>
            dumpGrid(full.get, client.listDataSources, Seq("id", "source","name", "dateCreated", "lastUpdated"), selector) }
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
          val grid = toggle()
          val full = toggle()
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get, grid.get, full.get) match {
            case selector => dumpGrid(full.get, client.listProperties, Seq("id", "name", "description", "propertyType/name", "unitOfMeasurement/name"), selector) }
        }
        val listPropertyTypes = new Subcommand("types") with Runnable{
          val grid = toggle()
          val full = toggle()
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get, grid.get, full.get) match {
            case selector => dumpGrid(full.get, client.listTypes, Seq("id", "name", "description"), selector) }
        }
        val listUnitsOfMeasurements = new Subcommand("units") with Runnable{
          val grid = toggle()
          val full = toggle()
          val filter = trailArg[String](required = false)
          override def run() =  checkJsonPointer(filter.get, grid.get, full.get) match {
            case selector => dumpGrid(full.get, client.listUnitsOfMeasurement, Seq("id", "name", "description"), selector) }
        }

      }

      val describeCommands = new Subcommand("describe") {

        val describeDataTable = new Subcommand("table") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(dataDict.getDataTableId(id()).flatMap { id =>
              client.describeDataTable(id)
            }, selector)
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
        val describePropertyType = new Subcommand("type") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector =>
              scala.util.control.Exception.allCatch.opt(id().toInt) match {
                case None => dumpJson(client.describeType(id()), selector)
                case Some(number) => dumpJson(client.describeType(number), selector)
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

        val dumpDataTable = new Subcommand("table") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {

            case selector => dumpJson(dataDict.getDataTableId(id()).flatMap { id =>
              client.dumpDataTable(id)}, selector)
          }
        }

        val dumpDataField = new Subcommand("field") with Runnable {
          val id = trailArg[String]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(dataDict.getFieldId(id()).flatMap { id => client.dumpDataField(id)}, selector)
          }
        }

        val dumpDataRecord = new Subcommand("record") with Runnable {
          val id = trailArg[Int]()
          val filter = trailArg[String](required = false)

          override def run() = checkJsonPointer(filter.get) match {
            case selector => dumpJson(client.dumpDataRecord(id()), selector)
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
        val createDataTable = new Subcommand("table") with Runnable {
          val definition = trailArg[String]()

          override def run() = _createDataTable(client, definition())
        }
        val createBundle = new Subcommand("bundle") with Runnable {
          val table = opt[String]()
          val entity = opt[Int]()
          val name = opt[String](required = true)

          override def run() = _createBundle(dataDict, name(), table.get, entity.get)
        }
        val createDataRecord = new Subcommand("record") with Runnable {
          val name = opt[String](required = true)
          val empty = toggle(default=Some(false))
          val fields = propsLong[String]("fields")
          override def run = _createDataRecord(dataDict, name(), empty(), fields.toIndexedSeq)
        }
        val createThing = new Subcommand("thing") with Runnable {
          val name = opt[String](required = true)
          val types = propsLong[Int]("types")
          override def run = dumpJson(dataDict.createThing(name(), types.toIndexedSeq))
        }
        val createProperty = new Subcommand("property") with Runnable {
          val name = opt[String](required = true)
          val ptype = opt[Int]("type", required= true)
          val unit = opt[Int](required = true)
          val description = opt[String]()
          override def run = dumpJson(dataDict.createProperty(name(), ptype(), unit(), description.get))
        }
      }
      val propose = new Subcommand("propose") {

        val proposeDataDebit = new Subcommand("dataDebit") with Runnable {
          val table = opt[String]()
          val entity = opt[Int]()
          val name = opt[String](required = true)

          override def run() = _proposeDataDebit(dataDict, name(), table.get, entity.get)
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

  private def _createDataRecord(client: HatDataDictionaryCache, name: String, empty: Boolean, fields: Seq[(String, String)]) = {
    if (empty && fields.nonEmpty) throw new IllegalArgumentException("use either --empty or --fields, not both")
    if (fields.isEmpty && !empty) throw new IllegalArgumentException("specify --fields for the record, or use --empty")
    dumpJson(client.createDataRecordWithNamedFields(name, fields))
  }

  private def _createBundle(client: HatDataDictionaryCache, name: String, table: Option[String], entityId: Option[Int] ) = {
    if (table != None && entityId != None) throw new IllegalArgumentException("cannot include both tables and entities in the same bundle");
    table match {
      case Some(t) => dumpJson(client.createContextlessBundle(name, t))
      case None => entityId match {
        case Some(e) => dumpJson(client.createContextualBundle(name, e))
        case None => throw new IllegalArgumentException("specify a table or an entity to create the bundle with")
      }
    }
  }

  private def _proposeDataDebit(client: HatDataDictionaryCache, name: String, table: Option[String], entityId:Option[Int]) = {
    if (table != None && entityId != None) throw new IllegalArgumentException("cannot include both tables and entities in the same bundle");
    table match {
      case Some(t) => dumpJson(client.getDataTableId(t).flatMap { id =>
        client.proposeDataDebit(name, new HatContextLessBundle(name, id),
          startDate = new Date()
        )})
      case None => entityId match {
        case Some(e) => dumpJson(client.proposeDataDebit(name, new HatContextualBundle(name, e)))
        case None => throw new IllegalArgumentException("specify a table or an entity to create the bundle with")
      }
    }

  }


  private def _rawPost(client: HatClient, path: String, fileName: String): Unit = {
    val entity = mapper.readValue[ObjectNode](new File(fileName))
    dumpJson(client.rawPost(path, entity))
  }

}


// from http://stackoverflow.com/a/7542476/14955
object Tabulator {
  def format(table: Seq[Seq[Any]]) = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length + 2)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]) = colSizes map { "-" * _ } mkString("+", "+", "+")
}