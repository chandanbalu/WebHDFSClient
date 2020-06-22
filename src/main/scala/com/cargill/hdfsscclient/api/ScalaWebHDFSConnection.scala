package com.cargill.hdfsscclient.api

import java.io._
import java.net.URL
import java.text.MessageFormat
import java.util
import java.util.Base64

import com.google.gson.Gson
import javax.net.ssl.HttpsURLConnection
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/22/2020, Mon
 **/
class ScalaWebHDFSConnection {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[ScalaWebHDFSConnection])
  val webHdfsConnectionFactory: ScalaWebHDFSConnectionFactory = new ScalaWebHDFSConnectionFactory
  private var httpfsUrl: String = webHdfsConnectionFactory.DEFAULT_PROTOCOL + webHdfsConnectionFactory.DEFAULT_HOST + ":" + webHdfsConnectionFactory.DEFAULT_PORT
  private var principal: String = webHdfsConnectionFactory.DEFAULT_USERNAME
  private var password: String = webHdfsConnectionFactory.DEFAULT_PASSWORD
  private var cdpEnv: String = webHdfsConnectionFactory.DEFAULT_CDPENV

  def this(httpfsUrl: String, cdpEnv: String, principal: String, password: String) {
    this()
    this.httpfsUrl = httpfsUrl
    this.cdpEnv = cdpEnv
    this.principal = principal
    this.password = password
  }

  @throws[IOException]
  private def responseMessage(conn: HttpsURLConnection, input: Boolean): String = {
    val sb: StringBuffer = new StringBuffer
    if (input) {
      val is: InputStream = conn.getInputStream
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(is))
      var line: String = null
      while ( (line = reader.readLine) != null) {
        sb.append(line)
      }
      reader.close()
      is.close()
    }
    val result: util.Map[String, Any] = new util.HashMap[String, Any]()
    result.put("code", conn.getResponseCode)
    result.put("message", conn.getResponseMessage)
    result.put("type", conn.getContentType)
    result.put("data", sb.toString)
    //
    // Convert a Map into JSON string.
    val gson: Gson = new Gson
    val json: String = gson.toJson(result)
    //logger.info("JSON Response = " + json);
    // Convert JSON string back to Map.
    // Type type = new TypeToken<Map<String, Object>>(){}.getType();
    // Map<String, Object> map = gson.fromJson(json, type);
    // for (String key : map.keySet()) {
    // System.out.println("map.get = " + map.get(key));
    // }
    return json
  }

  @throws[IOException]
  def getConnection(webHDFSURL: String, methodType: String): HttpsURLConnection = {
    val httpsURL: String = webHDFSURL
    val username: String = this.principal
    val password: String = this.password
    val userpass: String = username + ":" + password
    val basicAuth: String = "Basic " + new String(Base64.getEncoder.encode(userpass.getBytes))
    val cdpUrl: URL = new URL(httpsURL)
    val conn: HttpsURLConnection = cdpUrl.openConnection.asInstanceOf[HttpsURLConnection]
    // Indicate that we want to write to the HTTP request body
    if (conn.isInstanceOf[HttpsURLConnection]) {
      conn.setRequestProperty("Authorization", basicAuth)
      conn.setDoOutput(true)
      conn.setDoInput(true)
      conn.setRequestMethod(methodType)
      conn.setRequestProperty("Content-type", "text/plain")
    }
    logger.info("Establishing connection to webHDFS endpoint: " + this.httpfsUrl)
    return conn
  }

  ///user/ps784744/test_dir/titanic.csv
  @throws[IOException]
  def downloadFromHDFS(hdfsFileName: String, localFileName: String): Unit = { //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=OPEN"
    val appendURL: String = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=OPEN", this.cdpEnv, hdfsFileName)
    val webHDFSURL: String = this.httpfsUrl + appendURL
    logger.info("\nSending 'GET' request to URL : " + webHDFSURL)
    val conn: HttpsURLConnection = this.getConnection(webHDFSURL, "GET")
    conn.connect()
    // Create Output local file stream to write the HDFS file content
    try {
      val in: BufferedInputStream = new BufferedInputStream(conn.getInputStream)
      val fileOutputStream: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(localFileName))
      logger.info("Writing the file to local filesystem: " + localFileName)
      val dataBuffer: Array[Byte] = new Array[Byte](1024)
      var bytesRead: Int = 0
      while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -(1)) {
        fileOutputStream.write(dataBuffer, 0, bytesRead)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    val responseJSON: String = responseMessage(conn, false)
    conn.disconnect()
    val responseCode: Int = conn.getResponseCode
    logger.info("Response Code : " + responseCode)
    logger.info("JSON Response = " + responseJSON.toString)
  }

  @throws[IOException]
  def uploadFileToHDFS(localFileName: String, hdfsFileName: String): Unit = { //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=CREATE&overwrite=true";
    val appendURL: String = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=CREATE&overwrite=true", this.cdpEnv, hdfsFileName)
    val webHDFSURL: String = this.httpfsUrl + appendURL
    val conn: HttpsURLConnection = this.getConnection(webHDFSURL, "PUT")
    logger.info("\nSending 'PUT' request to URL : " + webHDFSURL)
    conn.connect()
    // Create Output HDFS stream to write the local file content
    try {
      val in: BufferedInputStream = new BufferedInputStream(new FileInputStream(localFileName))
      val fileOutputStream: BufferedOutputStream = new BufferedOutputStream(conn.getOutputStream)
      logger.info("Writing the file to HDFS: " + hdfsFileName)
      var bytesRead: Int = 0
      val dataBuffer: Array[Byte] = new Array[Byte](1024)
      // read byte by byte until end of stream
      while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -(1)) {
        fileOutputStream.write(dataBuffer, 0, bytesRead)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    val responseJSON: String = responseMessage(conn, false)
    conn.disconnect()
    val responseCode: Int = conn.getResponseCode
    logger.info("Response Code : " + responseCode)
    logger.info("JSON Response = " + responseJSON.toString)
  }

  @throws[IOException]
  def deleteFileOnHDFS(hdfsFileName: String): Unit = { //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=DELETE";
    val appendURL: String = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=DELETE", this.cdpEnv, hdfsFileName)
    val webHDFSURL: String = this.httpfsUrl + appendURL
    val conn: HttpsURLConnection = this.getConnection(webHDFSURL, "DELETE")
    logger.info("\nSending 'DELETE' request to URL : " + webHDFSURL)
    conn.connect()
    val responseJSON: String = responseMessage(conn, false)
    conn.disconnect()
    val responseCode: Int = conn.getResponseCode
    logger.info("Response Code : " + responseCode)
    logger.info("JSON Response = " + responseJSON.toString)
  }

  @throws[IOException]
  def getDirectoryStatus(hdfsFileName: String): Unit = { //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=LISTSTATUS";
    val appendURL: String = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=LISTSTATUS", this.cdpEnv, hdfsFileName)
    val webHDFSURL: String = this.httpfsUrl + appendURL
    val conn: HttpsURLConnection = this.getConnection(webHDFSURL, "GET")
    logger.info("\nSending 'GET' request to URL : " + webHDFSURL)
    conn.connect()
    val responseJSON: String = responseMessage(conn, false)
    conn.disconnect()
    val responseCode: Int = conn.getResponseCode
    logger.info("Response Code : " + responseCode)
    logger.info("JSON Response = " + responseJSON.toString)
  }

  @throws[IOException]
  def createDirectory(hdfsFileName: String): Unit = { //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir?op=MKDIRS";
    val appendURL: String = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=MKDIRS", this.cdpEnv, hdfsFileName)
    val webHDFSURL: String = this.httpfsUrl + appendURL
    val conn: HttpsURLConnection = this.getConnection(webHDFSURL, "PUT")
    logger.info("\nSending 'PUT' request to URL : " + webHDFSURL)
    conn.connect()
    val responseJSON: String = responseMessage(conn, false)
    conn.disconnect()
    val responseCode: Int = conn.getResponseCode
    logger.info("Response Code : " + responseCode)
    logger.info("JSON Response = " + responseJSON.toString)
  }
}
