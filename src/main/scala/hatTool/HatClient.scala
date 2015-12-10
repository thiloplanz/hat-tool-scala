package hatTool

import com.fasterxml.jackson.databind.JsonNode

import scala.concurrent.Future

trait HatClient{

  def listDataSources() : Future[JsonNode]

  protected def passwordParams(name:String, password:String)
    = Seq(("username", name), ("password", password))

}

private class HatOwnerClient(ning: NingJsonClient, host:String, name: String, password: String ) extends HatClient{

  override def listDataSources() =
    ning.get[JsonNode](host + "data/sources", queryParams = passwordParams(name, password))

}


object HatClient {

  def forOwner(ning: NingJsonClient, host: String, name: String, password: String) : HatClient
  = new HatOwnerClient(ning, host, name, password)

}