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

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId}
import java.util.Date

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.ning.http.client.Response

import scala.concurrent.{ExecutionContext, Future}

trait HatClient{

  // TODO: prepare proper Scala classes to return instead of raw JSON nodes

  type HatDataTable = ObjectNode
  type HatDataSource = ObjectNode
  type HatDataTableValues = ObjectNode
  type HatDataFieldValues = ObjectNode
  type HatDataRecordValues = ObjectNode
  type HatDataDebit = ObjectNode
  type HatEntity = ObjectNode
  type HatProperty = ObjectNode
  type HatPropertyType = ObjectNode
  type HatUnitOfMeasurement = ObjectNode


  def listDataSources() : Future[Seq[HatDataSource]]

  def listPersons() : Future[Seq[HatEntity]]

  def listThings() : Future[Seq[HatEntity]]

  def listLocations() : Future[Seq[HatEntity]]

  def listOrganizations() : Future[Seq[HatEntity]]

  def listEvents() : Future[Seq[HatEntity]]

  def listProperties() : Future[Seq[HatProperty]]

  def listPropertyTypes() : Future[Seq[HatPropertyType]]

  def listUnitsOfMeasurement() : Future[Seq[HatUnitOfMeasurement]]

  def describeDataTable(id:Int): Future[HatDataTable]

  def describeProperty(id: Int): Future[HatProperty]

  def describeProperty(name: String): Future[HatProperty]

  def describePropertyType(id: Int): Future[HatPropertyType]

  def describePropertyType(name: String): Future[HatPropertyType]

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

  def createContextlessBundle(name: String, tableId: Int): Future[JsonNode]

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


private abstract class HatClientBase(ning: NingJsonClient, host: String, extraQueryParams: Seq[(String, String)])(implicit val ec: ExecutionContext) extends HatClient {

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

  override def listPropertyTypes() = get[Seq[HatPropertyType]]("type/type")

  override def listUnitsOfMeasurement() = get[Seq[HatUnitOfMeasurement]]("type/unitofmeasurement")

  override def describeDataTable(id: Int) = get[HatDataTable]("data/table/"+id)

  override def describeProperty(id: Int) = get[HatProperty]("property/"+id)

  override def describeProperty(name: String) = get[Seq[HatProperty]]("property", queryParams = Seq("name" -> name)).map {
    case Seq() => throw new IllegalArgumentException(s"there is no property called '${name}'")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} properties called '${name}'")
  }

  // there is no get by ID endpoint
  override def describePropertyType(id: Int) = listPropertyTypes().map { _.filter { _.get("id").asInt == id} match {
    case Seq() => throw new IllegalArgumentException(s"there is no property type with id ${id}")
    case Seq(one) => one
    case many => throw new IllegalArgumentException(s"there are ${many.size} property types with id ${id}")
  }}

  override def describePropertyType(name: String) = get[Seq[HatProperty]]("type/type", queryParams = Seq("name" -> name)).map {
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

  override def dumpDataRecord(id: Int) = get[HatDataRecordValues]("data/record/"+id+"/values")

  override def getPerson(id: Int) = get[HatEntity]("person/"+id+"/values")

  override def getThing(id: Int) = get[HatEntity]("thing/"+id+"/values")

  override def getLocation(id: Int) = get[HatEntity]("location/"+id+"/values")

  override def getOrganization(id: Int) = get[HatEntity]("organisation/"+id+"/values")

  override def getEvent(id: Int) = get[HatEntity]("event/"+id+"/values")

  override def createDataTable(definition: ObjectNode) = post[HatDataTable]("data/table", definition, okayStatusCode = 201)

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

}