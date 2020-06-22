package com.cargill.hdfsscclient

import com.cargill.hdfsscclient.util.ArgumentScalaParser
import org.apache.commons.cli.ParseException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.net.ssl.HttpsURLConnection
import java.io._
import java.net.URL
import java.util.Base64
import java.util.Properties

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/22/2020, Mon
 **/
object UploadScalaWebHDFSClient {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @throws[IOException]
  @throws[ParseException]
  def main(args: Array[String]): Unit = {
    val argParser: ArgumentScalaParser = new ArgumentScalaParser("Console", args)
    val argsProp: Properties = argParser.getConsoleArgsProperties
    val DEFAULT_PROTOCOL: String = "https://"
    val DEFAULT_CDPENV: String = "drona"
    val host: String = argsProp.getProperty("host")
    val port: String = argsProp.getProperty("port")
    val username: String = argsProp.getProperty("username")
    val password: String = argsProp.getProperty("password")
    val hdfsFileName: String = argsProp.getProperty("hdfsfile")
    val localFileName: String = argsProp.getProperty("localfile")
    if (host == null || port == null || username == null || password == null || hdfsFileName == null || localFileName == null) {
      argParser.parserError("Console")
    }
    //https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir?op=LISTSTATUS
    val httpsURL: String = DEFAULT_PROTOCOL + host + ":" + port + "/gateway/" + DEFAULT_CDPENV + "/webhdfs/v1" + hdfsFileName + "?op=CREATE&overwrite=true"
    val userpass: String = username + ":" + password
    val basicAuth: String = "Basic " + new String(Base64.getEncoder.encode(userpass.getBytes))
    val myUrl: URL = new URL(httpsURL)
    val conn: HttpsURLConnection = myUrl.openConnection.asInstanceOf[HttpsURLConnection]
    logger.info("Establishing connection to webHDFS endpoint: " + httpsURL)
    // Indicate that we want to write to the HTTP request body
    if (conn.isInstanceOf[HttpsURLConnection]) {
      conn.setRequestProperty("Authorization", basicAuth)
      conn.setDoOutput(true)
      conn.setDoInput(true)
      conn.setRequestMethod("PUT")
      conn.setRequestProperty("Content-type", "text/plain")
      conn.connect()
    }
    // Create Output HDFS stream to write the local file content
    try {
      val in: BufferedInputStream = new BufferedInputStream(new FileInputStream(localFileName))
      val fileOutputStream: BufferedOutputStream = new BufferedOutputStream(conn.getOutputStream)
      logger.info("Writing the file to HDFS: " + hdfsFileName)
      var bytesRead: Int = 0
      val dataBuffer: Array[Byte] = new Array[Byte](1024)
      // read byte by byte until end of stream
      while ( {
        (bytesRead = in.read(dataBuffer, 0, 1024)) != -(1)
      }) {
        fileOutputStream.write(dataBuffer, 0, bytesRead)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    val sb: StringBuffer = new StringBuffer
    if (false) {
      val is: InputStream = conn.getInputStream
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(is))
      var line: String = null
      while ( {
        (line = reader.readLine) != null
      }) {
        sb.append(line)
      }
      reader.close()
      is.close()
    }
    conn.disconnect()
    val responseCode: Int = conn.getResponseCode
    logger.info("Response Code : " + responseCode)
  }
}
