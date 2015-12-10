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

  def listDataSources() : Future[Seq[ObjectNode]]

  def describeDataTable(id:Int): Future[ObjectNode]

  def dumpDataTable(id:Int): Future[Seq[ObjectNode]]

}

private abstract class HatClientBase(ning: NingJsonClient, host:String) extends HatClient {

  def get[T:Manifest](path: String, queryParams: Seq[(String, String)] = Seq.empty) : Future[T]

  override def listDataSources() = get[Seq[ObjectNode]]("data/sources")

  override def describeDataTable(id: Int) = get[ObjectNode]("data/table/"+id)

  override def dumpDataTable(id: Int) = get[Seq[ObjectNode]]("data/table/"+id+"/values")

}

private class HatOwnerClient(ning: NingJsonClient, host:String, name: String, password: String ) extends HatClientBase(ning, host){

  private def passwordParams(name:String, password:String)  = Seq("username"-> name, "password" -> password)

  override def get[T:Manifest](path: String, queryParams: Seq[(String, String)] = Seq.empty) =
    ning.get[T](host + path, queryParams = queryParams ++ passwordParams(name, password))

}


object HatClient {

  def forOwner(ning: NingJsonClient, host: String, name: String, password: String) : HatClient = new HatOwnerClient(ning, host, name, password)

}