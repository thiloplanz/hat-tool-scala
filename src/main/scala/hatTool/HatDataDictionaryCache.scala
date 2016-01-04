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

import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import hatTool.HatClient.{HatDataRecordValues, HatDataFieldValues}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Tool to look up (and cache) the HAT data dictionary object ids (for tables and fields)
 */

class HatDataDictionaryCache(client:HatClient)(implicit ec: ExecutionContext) extends DelegatingHatClient(client) {

  // TODO: also cache the table and field description JSON
  private val cache = new ConcurrentHashMap[String, Int]

  private def updateCache(name: String, id: Future[Int]) = { id.map{ id => cache.put(name, id); id } }

  private def getCached(name: String) = Option(cache.get(name)).map(Future.successful(_))

  /**
   * @param tableNameOrId can be numeric (assumed to be the ID already), or source:tableName
   *
   * fails if there is no such table
   */
  def getDataTableId(tableNameOrId: String) : Future[Int] =
    Try(tableNameOrId.toInt).toOption match {
      case Some(id) => Future.successful(id)
      case None => tableNameOrId.split(':') match {
        case Array(source, name) => getCached(tableNameOrId).getOrElse(updateCache(tableNameOrId, client.getDataTableId(source, name)));
        case _ => throw new IllegalArgumentException("invalid tableNameOrId, should be source:name "+tableNameOrId)
      }
    }

  /**
   *
   * @param fieldNameOrId can be numeric (assumed to be the ID already), or source:tableName:fieldName or tableId:fieldName
   * @return
   */
  def getFieldId(fieldNameOrId: String) : Future[Int] =
    Try(fieldNameOrId.toInt).toOption match {
      case Some(id) => Future.successful(id)
      case None => fieldNameOrId.split(':') match {
        case Array(tableId, fieldName) => getFieldId(tableId, fieldName)
        case Array(source, tableName, fieldName) => getFieldId(source+":"+tableName, fieldName)
        case _ => throw new IllegalArgumentException("invalid fieldNameOrId, should be source:tableName:fieldName "+fieldNameOrId)
      }
    }


  // TODO: stop using JSON, have some proper beans
  def getFieldId(tableNameOrId: String, fieldName: String) : Future[Int] =
    getDataTableId(tableNameOrId).flatMap { tableId =>
      client.describeDataTable(tableId).map { tableJson =>
        import scala.collection.JavaConversions._
        tableJson.get("fields").asInstanceOf[ArrayNode].find(_.get("name").asText == fieldName) match {
          case None => throw new IllegalArgumentException("no field named '"+fieldName+"' in table "+tableId)
          case Some(field) => field.get("id").asInt
        }
      }
    }


   // improved HatClient methods that can now accept names instead of ids

  def createContextlessBundle(name: String, tableId: String) : Future[JsonNode] = getDataTableId(tableId).flatMap{ id => createContextlessBundle(name, id) }


  def createDataRecordWithNamedFields(name: String, fields: Seq[ (String, String)]) : Future[HatDataRecordValues] =
  // TODO: also get the proper field name (but currently HAT does not care anyway, see https://github.com/Hub-of-all-Things/HAT2.0/issues/19
    Future.sequence(fields.map{ case( field, value) => getFieldId(field).map{ id => (id, field, value)}}).flatMap{ fields =>
      createDataRecord(name, fields)
    }


}
