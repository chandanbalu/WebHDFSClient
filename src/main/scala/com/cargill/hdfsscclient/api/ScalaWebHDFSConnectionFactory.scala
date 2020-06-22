package com.cargill.hdfsscclient.api

import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/22/2020, Mon
 **/
class ScalaWebHDFSConnectionFactory {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  /** The default CDP to connect to */
  val DEFAULT_CDPENV = "drona"

  /** The default host to connect to */
  val DEFAULT_HOST = "drona-haproxy.cargill.com"

  /** The default port */
  val DEFAULT_PORT = "8443"

  /** The default username */
  val DEFAULT_USERNAME = "ps784744"

  val DEFAULT_PASSWORD = "password"

  val DEFAULT_PROTOCOL = "https://"

  object AuthenticationType extends Enumeration {
    type AuthenticationType = Value
    val KERBEROS, SIMPLE = Value
  }

  private var cdpEnv = DEFAULT_CDPENV
  private var host = DEFAULT_HOST
  private var port = DEFAULT_PORT
  private var username = DEFAULT_USERNAME
  private var password = DEFAULT_PASSWORD
  private var authenticationType = this.AuthenticationType.KERBEROS.toString
  private val webHDFSConnection = null

  def ScalaWebHDFSConnectionFactory(cdpEnv: String, host: String, port: String, username: String, password: String, authType: String) {
    this.cdpEnv = cdpEnv
    this.host = host
    this.port = port
    this.username = username
    this.password = password
    this.authenticationType = authType
  }

  def getConnection: ScalaWebHDFSConnection = {
    val httpfsUrl = this.DEFAULT_PROTOCOL + this.host + ":" + this.port
    val webHdfsConnection = new ScalaWebHDFSConnection(httpfsUrl, this.cdpEnv, this.username, this.password)
    webHdfsConnection
  }

  def getHost: String = host

  def setHost(host: String): Unit = {
    this.host = host
  }

  def getPort: String = port

  def setPort(port: String): Unit = {
    this.port = port
  }

  def getUsername: String = username

  def setUsername(username: String): Unit = {
    this.username = username
  }

  def getPassword: String = password

  def setPassword(password: String): Unit = {
    this.password = password
  }

  def getAuthenticationType: String = authenticationType

  def setAuthenticationType(authenticationType: String): Unit = {
    this.authenticationType = authenticationType
  }
}
