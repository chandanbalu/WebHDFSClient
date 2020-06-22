package com.cargill.hdfsscclient

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import com.cargill.hdfsscclient.api.ScalaWebHDFSConnection
import com.cargill.hdfsscclient.api.ScalaWebHDFSConnectionFactory
import com.cargill.hdfsscclient.util.ArgumentScalaParser

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/22/2020, Mon
 **/
object ScalaWebHDFSClient {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(classOf[ScalaWebHDFSConnection])
    // Read the arguments using Parser
    val argParser: ArgumentScalaParser = new ArgumentScalaParser("Simple", args)
    val argsProp: Properties = argParser.getSimpleArgsProperties
    val hdfsOperation: String = argsProp.getProperty("hdfsoperation")
    val hdfsFileName: String = argsProp.getProperty("hdfsfilename")
    val localFileName: String = argsProp.getProperty("localfilename")
    if (hdfsOperation == null) {
      argParser.parserError("Simple")
    }
    if (hdfsOperation == "download" || hdfsOperation == "upload") {
      if (hdfsFileName == null || localFileName == null) {
        argParser.parserError("Simple")
      }
    }
    if (hdfsOperation == "create" || hdfsOperation == "list" || hdfsOperation == "delete") {
      if (hdfsFileName == null) {
        argParser.parserError("Simple")
      }
    }
    /** Using the Factory build the hdfsConnection object */
    val webHdfsConnectionFactory: ScalaWebHDFSConnectionFactory = new ScalaWebHDFSConnectionFactory
    val webHdfsConnection: ScalaWebHDFSConnection = webHdfsConnectionFactory.getConnection
    hdfsOperation match {
      case "upload" =>
        webHdfsConnection.uploadFileToHDFS(localFileName, hdfsFileName)

      case "download" =>
        webHdfsConnection.downloadFromHDFS(hdfsFileName, localFileName)

      case "create" =>
        webHdfsConnection.createDirectory(hdfsFileName)

      case "list" =>
        webHdfsConnection.getDirectoryStatus(hdfsFileName)

      case "delete" =>
        webHdfsConnection.deleteFileOnHDFS(hdfsFileName)

      case _ =>
        logger.info("Invalid or Unsupported HDFS operation")
    }
  }
}
