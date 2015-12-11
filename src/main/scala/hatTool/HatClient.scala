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

import com.fasterxml.jackson.databind.node.ObjectNode

import scala.concurrent.Future

trait HatClient{

  // TODO: prepare proper Scala classes to return instead of raw JSON nodes

  type HatDataTable = ObjectNode
  type HatDataSource = ObjectNode
  type HatDataTableValues = ObjectNode

  def listDataSources() : Future[Seq[HatDataSource]]

  def describeDataTable(id:Int): Future[HatDataTable]

  def dumpDataTable(id:Int): Future[Seq[HatDataTableValues]]

  def createDataTable(definition: HatDataTable): Future[HatDataTable]

}

private abstract class HatClientBase extends HatClient {

  def get[T:Manifest](path: String) : Future[T]

  def post[T:Manifest](path: String, entity: Any, okayStatusCode: Int = 200) : Future[T]

  override def listDataSources() = get[Seq[HatDataSource]]("data/sources")

  override def describeDataTable(id: Int) = get[HatDataTable]("data/table/"+id)

  override def dumpDataTable(id: Int) = get[Seq[HatDataTableValues]]("data/table/"+id+"/values")

  override def createDataTable(definition: HatDataTable) = post[HatDataTable]("data/table", definition, okayStatusCode = 201)

}

private class HatOwnerClient(ning: NingJsonClient, host:String, name: String, password: String ) extends HatClientBase{

  private val passwordParams  = Seq("username"-> name, "password" -> password)

  override def get[T:Manifest](path: String) =
    ning.get[T](host + path, queryParams = passwordParams)

  override def post[T:Manifest](path: String, entity: Any, okayStatusCode: Int = 200) =
    ning.postJson[T](host+path, entity, queryParams = passwordParams, okayStatusCode = okayStatusCode)

}


object HatClient {

  def forOwner(ning: NingJsonClient, host: String, name: String, password: String) : HatClient = new HatOwnerClient(ning, host, name, password)

}