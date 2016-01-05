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

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId}
import java.util.Date

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.ning.http.client.Response

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait HatClient{

  import HatClient._

  def listDataSources() : Future[Seq[HatDataSource]]

  def listPersons() : Future[Seq[HatEntity]]

  def listThings() : Future[Seq[HatEntity]]

  def listLocations() : Future[Seq[HatEntity]]

  def listOrganizations() : Future[Seq[HatEntity]]

  def listEvents() : Future[Seq[HatEntity]]

  def listProperties() : Future[Seq[HatProperty]]

  def listTypes() : Future[Seq[HatType]]

  def listUnitsOfMeasurement() : Future[Seq[HatUnitOfMeasurement]]

  def describeDataTable(id:Int): Future[HatDataTable]

  def describeProperty(id: Int): Future[HatProperty]

  def describeProperty(name: String): Future[HatProperty]

  def describeType(id: Int): Future[HatType]

  def describeType(name: String): Future[HatType]

  def describeUnitOfMeasurement(id: Int): Future[HatUnitOfMeasurement]

  def describeUnitOfMeasurement(name: String): Future[HatUnitOfMeasurement]

  def getDataTableName(id:Int): Future[HatDataTableName]

  def dumpDataTable(id:Int): Future[Seq[HatDataTableValues]]

  def dumpDataField(id:Int): Future[HatDataFieldValues]

  def dumpDataRecord(id:Int): Future[HatDataRecordValues]

  def getPerson(id:Int): Future[HatEntity]

  def getThing(id:Int): Future[HatEntity]

  def getLocation(id:Int): Future[HatEntity]

  def getOrganization(id:Int): Future[HatEntity]

  def getEvent(id:Int): Future[HatEntity]

  def createDataTable(definition: HatDataTable): Future[HatDataTable]

  /**
   *
   * @param name
   * @param fields a sequence of fieldId , fieldName => value mappings
   * @return
   */
  def createDataRecord(name: String, fields: Seq[(Int, String, String)]) : Future[HatDataRecordValues]

  def createThing(name: String, types: Seq[(String,Int)] = Seq.empty) : Future[HatEntity]

  def createContextlessBundle(name: String, tableId: Int): Future[JsonNode]

  def addTypesToThing(id: Int, types: Seq[(String,Int)]) : Future[Unit]

  def proposeDataDebit(name: String,
                       bundle: HatBundleDefinition,
                       startDate: Date = new Date(),
                       validity: Duration = Duration.ofDays(3650),
                       rolling: Boolean = false,
                       sell: Boolean = false,
                       price: Float = 0
                       ) : Future[HatDataDebit]

  def enableDataDebit(key: String) : Future[String]

  def disableDataDebit(key: String) : Future[String]

  def dumpDataDebitValues(key: String): Future[JsonNode]

  def rawPost(path: String, entity: JsonNode): Future[JsonNode]

  def getDataTableId(source: String, name: String) : Future[Int]

}

trait HatBundleDefinition{
   def name: String
   def kind: String
   def payload(client:HatClient)(implicit ec: ExecutionContext) : Future[(String, Map[_,_])]
}

case class HatContextLessBundle(name: String, tableId: Int) extends HatBundleDefinition {
  val kind = "contextless"
  def payload(client:HatClient)(implicit ec: ExecutionContext) = client.getDataTableName(tableId).map { tableInfo =>
    "bundleContextless" -> Map("name" -> name, "tables" -> table(tableInfo))
  }
  def table(tableInfo: HatDataTableName) = Seq(
    Map("name" -> name, "bundleTable" ->
      Map("name" -> name, "table"-> Map("id" -> tableId, "name" -> tableInfo.name, "source" -> tableInfo.source))))
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class HatDataTableName(
                       name: String,
                       source: String,
                       id: Int
                         )


abstract class DelegatingHatClient(delegate: HatClient) extends HatClient {
  import HatClient._

  override def listDataSources()  = delegate.listDataSources()
  override def listPersons()= delegate.listPersons()
  override def listThings() = delegate.listThings()
  override def listLocations() = delegate.listLocations()
  override def listOrganizations() = delegate.listOrganizations()
  override def listEvents() = delegate.listEvents()
  override def listProperties() = delegate.listProperties()
  override def listTypes() = delegate.listTypes()
  override def listUnitsOfMeasurement() = delegate.listUnitsOfMeasurement()
  override def describeDataTable(id:Int)= delegate.describeDataTable(id)
  override def describeProperty(id: Int)= delegate.describeProperty(id)
  override def describeProperty(name: String)= delegate.describeProperty(name)
  override def describeType(id: Int)= delegate.describeType(id)
  override def describeType(name: String)= delegate.describeType(name)
  override def describeUnitOfMeasurement(id: Int)= delegate.describeUnitOfMeasurement(id)
  override def describeUnitOfMeasurement(name: String)= delegate.describeUnitOfMeasurement(name)
  override def getDataTableName(id:Int)= delegate.getDataTableName(id)
  override def dumpDataTable(id:Int)= delegate.dumpDataTable(id)
  override def dumpDataField(id:Int)= delegate.dumpDataField(id)
  override def dumpDataRecord(id:Int)= delegate.dumpDataRecord(id)
  override def getPerson(id:Int)= delegate.getPerson(id)
  override def getThing(id:Int)= delegate.getThing(id)
  override def getLocation(id:Int)= delegate.getLocation(id)
  override def getOrganization(id:Int)= delegate.getOrganization(id)
  override def getEvent(id:Int)= delegate.getEvent(id)
  override def createDataTable(definition: HatDataTable)= delegate.createDataTable(definition)
  override def createDataRecord(name: String, fields: Seq[(Int, String, String)]) = delegate.createDataRecord(name, fields)
  override def createThing(name: String, types: Seq[(String, Int)] = Seq.empty) = delegate.createThing(name, types)
  override def createContextlessBundle(name: String, tableId: Int)= delegate.createContextlessBundle(name, tableId)
  override def addTypesToThing(id: Int, types: Seq[(String, Int)]) = delegate.addTypesToThing(id, types)
  override def proposeDataDebit(name: String,
                       bundle: HatBundleDefinition,
                       startDate: Date = new Date(),
                       validity: Duration = Duration.ofDays(3650),
                       rolling: Boolean = false,
                       sell: Boolean = false,
                       price: Float = 0
                        ) = delegate.proposeDataDebit(name, bundle, startDate, validity, rolling, sell, price)
  override def enableDataDebit(key: String) = delegate.enableDataDebit(key)
  override def disableDataDebit(key: String) = delegate.disableDataDebit(key)
  override def dumpDataDebitValues(key: String)= delegate.dumpDataDebitValues(key)
  override def rawPost(path: String, entity: JsonNode)= delegate.rawPost(path, entity)
  override def getDataTableId(source: String, name: String) = delegate.getDataTableId(source, name)
}


private abstract class HatClientBase(ning: NingJsonClient, host: String, extraQueryParams: Seq[(String, String)])(implicit val ec: ExecutionContext) extends HatClient {

  import HatClient._

  def get[T:Manifest](path: String, queryParams: Seq[(String, String)] = Map.empty.toList)  =
    ning.get[T](host + path, queryParams = extraQueryParams ++ queryParams)

  def post[T:Manifest](path: String, entity: Any, okayStatusCode: Int = 200) =
    ning.postJson[T](host+path, entity, queryParams = extraQueryParams, okayStatusCode = okayStatusCode)

  def put[T:Manifest](path: String, entity: Any, okayStatusCode: Int = 200) =
    ning.putJson[T](host+path, entity, queryParams = extraQueryParams, okayStatusCode = okayStatusCode)


  private val compactISO8601WithoutMilliSeconds = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault)

  override def listDataSources() = get[Seq[HatDataSource]]("data/sources")

  override def listPersons() = get[Seq[HatEntity]]("person")

  override def listThings() = get[Seq[HatEntity]]("thing")

  override def listEvents() = get[Seq[HatEntity]]("event")

  override def listLocations() = get[Seq[HatEntity]]("location")

  override def listOrganizations() = get[Seq[HatEntity]]("organisation")

  override def listProperties() = get[Seq[HatProperty]]("property")

  override def listTypes() = get[Seq[HatType]]("type/type")

  override def listUnitsOfMeasurement() = get[Seq[HatUnitOfMeasurement]]("type/unitofmeasurement")

  override def describeDataTable(id: Int) = get[HatDataTable]("data/table/"+id)

  override def describeProperty(id: Int) = get[HatProperty]("property/"+id)

  override def describeProperty(name: String) = get[Seq[HatProperty]]("property", queryParams = Seq("name" -> name)).map {
    case Seq() => throw new IllegalArgumentException(s"there is no property called '${name}'")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} properties called '${name}'")
  }

  // there is no get by ID endpoint
  override def describeType(id: Int) = listTypes().map { _.filter { _.get("id").asInt == id} match {
    case Seq() => throw new IllegalArgumentException(s"there is no property type with id ${id}")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} property types with id ${id}")
  }}

  override def describeType(name: String) = get[Seq[HatProperty]]("type/type", queryParams = Seq("name" -> name)).map {
    case Seq() => throw new IllegalArgumentException(s"there is no property type called '${name}'")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} properties types called '${name}'")
  }

  // there is no get by ID endpoint
  override def describeUnitOfMeasurement(id: Int) = listUnitsOfMeasurement().map { _.filter { _.get("id").asInt == id} match {
    case Seq() => throw new IllegalArgumentException(s"there is no unit with id ${id}")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} units with id ${id}")
  }}

  override def describeUnitOfMeasurement(name: String) = get[Seq[HatProperty]]("type/unitofmeasurement", queryParams = Seq("name" -> name)).map {
    case Seq() => throw new IllegalArgumentException(s"there is no unit called '${name}'")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} units called '${name}'")
  }

  override def getDataTableName(id: Int) = get[HatDataTableName]("data/table/"+id)

  override def dumpDataTable(id: Int) = get[Seq[HatDataTableValues]]("data/table/"+id+"/values")

  override def dumpDataField(id: Int) = get[HatDataFieldValues]("data/field/"+id+"/values")

  override def dumpDataRecord(id: Int) = get[HatDataRecordValues]("data/record/"+id+"/values").recoverWith {
    // this will return 404 if there are no values, so we try to get the record itself
    case x: UnsuccessfulRequestException => get[HatDataRecordValues]("data/record/"+id)
  }

  override def getPerson(id: Int) = get[HatEntity]("person/"+id+"/values")

  override def getThing(id: Int) = get[HatEntity]("thing/"+id+"/values")

  override def getLocation(id: Int) = get[HatEntity]("location/"+id+"/values")

  override def getOrganization(id: Int) = get[HatEntity]("organisation/"+id+"/values")

  override def getEvent(id: Int) = get[HatEntity]("event/"+id+"/values")

  override def createDataTable(definition: ObjectNode) = post[HatDataTable]("data/table", definition, okayStatusCode = 201)

  override def createDataRecord(name: String, fields: Seq[(Int, String, String)]) =
    fields match {
      case Seq() =>  post[HatDataRecordValues]("data/record", Map("name" -> name), okayStatusCode = 201)
      case _ => post[HatDataRecordValues]("data/record/values", Map("record" -> Map("name" -> name),
      "values" -> fields.map{case (f, n, v) => Map("field" -> Map("id" -> f, "name" -> n), "value" -> v) }), okayStatusCode = 201)
    }


  private def addTypesToEntity(id: Int, kind: String, types:Seq[(String,Int)]) : Future[Unit] =
    Future.sequence(types.map { case (r, t) =>
      post[JsonNode](kind+"/"+id+"/type/"+t, Map("relationshipType" ->r ), okayStatusCode = 201)
    }).map{_ => None}

  override def addTypesToThing(id: Int, types: Seq[(String,Int)]) : Future[Unit] = addTypesToEntity(id, "thing", types)


  // TODO: should be a single API call, https://github.com/Hub-of-all-Things/HAT2.0/issues/20
  override def createThing(name: String, types: Seq[(String,Int)] = Seq.empty) =
    post[HatEntity]("thing", Map("name" -> name), okayStatusCode = 201).flatMap{ entity => addTypesToThing(entity.get("id").asInt, types).map{ _ => entity} }



  override def createContextlessBundle(name: String, tableId: Int) = {
    // first we need the table information
    getDataTableName(tableId).flatMap { tableInfo =>
      post[JsonNode]("bundles/contextless",
        Map("name" -> name, "tables" -> HatContextLessBundle(name, tableId).table(tableInfo)),
        okayStatusCode = 201)
    }
  }

  override def proposeDataDebit(name: String,
                                bundle: HatBundleDefinition,
                                startDate: Date = new Date(),
                                validity: Duration = Duration.ofDays(3650),
                                rolling: Boolean = false,
                                sell: Boolean = false,
                                price: Float = 0
                                 ) =
    bundle.payload(this).flatMap { payload =>
      post[HatDataDebit]("dataDebit/propose", Map(
      "name" -> name,
      "kind" -> bundle.kind,
      "startDate" -> compactISO8601WithoutMilliSeconds.format(startDate.toInstant),
      "endDate" -> compactISO8601WithoutMilliSeconds.format(startDate.toInstant.plus(validity)),
      "rolling" -> false,
      "sell" -> true,
      "price" -> price,
       payload
      ), okayStatusCode = 201)
    }

  override def enableDataDebit(key: String) = put[Response]("dataDebit/"+key+"/enable", true).map { response =>
    if (response.getStatusCode != 200) throw new UnsuccessfulRequestException(HttpStatus(response.getStatusCode, response.getStatusText, "dataDebit/"+key+"/enable", Seq.empty))
    response.getResponseBody("UTF-8")
  }

  override def disableDataDebit(key: String) = put[Response]("dataDebit/"+key+"/disable", false).map { response =>
    if (response.getStatusCode != 200) throw new UnsuccessfulRequestException(HttpStatus(response.getStatusCode, response.getStatusText, "dataDebit/"+key+"/disable", Seq.empty))
    response.getResponseBody("UTF-8")
  }

  override def dumpDataDebitValues(key: String) = get[JsonNode]("dataDebit/"+key+"/values")

  override def rawPost(path: String, entity: JsonNode) = post[JsonNode](path, entity)

  override def getDataTableId(source: String, name: String) =
    get[HatDataTableName]("data/table", queryParams = Seq( "source" -> source, "name" -> name)).map{ _.id}


}

private class HatOwnerClient(ning: NingJsonClient, host:String, name: String, password: String, ec: ExecutionContext)
  extends HatClientBase(ning, host, Seq("username"-> name, "password" -> password))(ec)

private class AccessTokenClient(ning: NingJsonClient, host:String, accessToken: String, ec: ExecutionContext)
  extends HatClientBase(ning, host, Seq("access_token" -> accessToken))(ec)

object HatClient {

  def forOwner(ning: NingJsonClient, host: String, name: String, password: String)(implicit ec: ExecutionContext) : HatClient
  = new HatOwnerClient(ning, host, name, password, ec)

  def forAccessToken(ning: NingJsonClient, host: String, accessToken: String)(implicit ec: ExecutionContext) : HatClient
  = new AccessTokenClient(ning, host, accessToken, ec)

  // TODO: prepare proper Scala classes to return instead of raw JSON nodes

  final type HatDataTable = ObjectNode
  final type HatDataSource = ObjectNode
  final type HatDataTableValues = ObjectNode
  final type HatDataFieldValues = ObjectNode
  final type HatDataRecordValues = ObjectNode
  final type HatDataDebit = ObjectNode
  final type HatEntity = ObjectNode
  final type HatProperty = ObjectNode
  final type HatType = ObjectNode
  final type HatUnitOfMeasurement = ObjectNode

}