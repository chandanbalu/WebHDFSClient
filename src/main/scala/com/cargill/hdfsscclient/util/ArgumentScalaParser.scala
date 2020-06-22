package com.cargill.hdfsscclient.util

import com.cargill.hdfsclient.api.WebHDFSConnection
import org.apache.commons.cli._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/22/2020, Mon
 **/
class ArgumentScalaParser(optionType: String, args: Array[String]){
  protected val logger: Logger = LoggerFactory.getLogger(classOf[WebHDFSConnection])
  private[util] var argsConsole: Array[String] = null
  private[util] var argsSimple: Array[String] = null
  private[util] val optionsConsole: Options = new Options
  private[util] val optionsSimple: Options = new Options

  def parserError(optionType: String): Unit = {
    logger.info("Error parsing command-line arguments!")
    logger.info("Please, follow the instructions below:")
    val formatter: HelpFormatter = new HelpFormatter
    if (optionType eq "Console") {
      formatter.printHelp("webHDFS client", optionsConsole)
    }
    else {
      if (optionType eq "Simple") {
        formatter.printHelp("webHDFS client", optionsSimple)
      }
      else {
        logger.info("Error initializing the ArgumentParser.")
        System.exit(1)
      }
    }
    System.exit(1)
  }

  @throws[ParseException]
  def getConsoleArgsProperties: Properties = {
    val host: Option = new Option("h", "host", true, "Hostname ([REQUIRED] or use --host)")
    host.setRequired(true)
    optionsConsole.addOption(host)
    val port: Option = new Option("n", "port", true, "Portnumber ([REQUIRED] or use --port)")
    port.setRequired(true)
    optionsConsole.addOption(port)
    val username: Option = new Option("u", "username", true, "Username ([REQUIRED] or use --username)")
    username.setRequired(true)
    optionsConsole.addOption(username)
    val password: Option = new Option("p", "password", true, "Hostname ([REQUIRED] or use --password)")
    password.setRequired(true)
    optionsConsole.addOption(password)
    val hdfsfile: Option = new Option("hf", "hdfsfile", true, "HDFS Filename ([OPTIONAL] or use --hdfsfile)")
    hdfsfile.setRequired(false)
    optionsConsole.addOption(hdfsfile)
    val localfile: Option = new Option("lf", "localfile", true, "Local Filename ([OPTIONAL] or use --localfile)")
    localfile.setRequired(false)
    optionsConsole.addOption(localfile)
    val parser: CommandLineParser = new DefaultParser
    val argsProp: Properties = new Properties
    try {
      val cmd: CommandLine = parser.parse(optionsConsole, argsConsole)
      if (cmd.hasOption("h")) {
        val hostName: String = cmd.getOptionValue("h")
        argsProp.setProperty("host", hostName)
      }
      if (cmd.hasOption("n")) {
        val portNumber: String = cmd.getOptionValue("n")
        argsProp.setProperty("port", portNumber)
      }
      if (cmd.hasOption("u")) {
        val userName: String = cmd.getOptionValue("u")
        argsProp.setProperty("username", userName)
      }
      if (cmd.hasOption("p")) {
        val passWord: String = cmd.getOptionValue("p")
        argsProp.setProperty("password", passWord)
      }
      if (cmd.hasOption("hf")) {
        val hdfsFile: String = cmd.getOptionValue("hf")
        argsProp.setProperty("hdfsfile", hdfsFile)
      }
      if (cmd.hasOption("lf")) {
        val localFile: String = cmd.getOptionValue("lf")
        argsProp.setProperty("localfile", localFile)
      }
    } catch {
      case e: ParseException =>
        logger.info("Error parsing command-line arguments!")
        logger.info("Please, follow the instructions below:")
        val formatter: HelpFormatter = new HelpFormatter
        formatter.printHelp("webHDFS client", optionsConsole)
        System.exit(1)
    }
    return argsProp
  }

  @throws[ParseException]
  def getSimpleArgsProperties: Properties = {
    val hdfsoperation: Option = new Option("o", "operation", true, "HDFS Operation to be Performed ([REQUIRED] or use --operation)")
    hdfsoperation.setRequired(true)
    optionsSimple.addOption(hdfsoperation)
    val hdfsfile: Option = new Option("hf", "hdfsfile", true, "HDFS Filename ([OPTIONAL] or use --hdfsfile)")
    hdfsfile.setRequired(false)
    optionsSimple.addOption(hdfsfile)
    val localfile: Option = new Option("lf", "localfile", true, "Local Filename ([OPTIONAL] or use --localfile)")
    localfile.setRequired(false)
    optionsSimple.addOption(localfile)
    val parser: CommandLineParser = new DefaultParser
    val argsProp: Properties = new Properties
    try {
      val cmd: CommandLine = parser.parse(optionsSimple, argsSimple)
      if (cmd.hasOption("o")) {
        val hdfsOperation: String = cmd.getOptionValue("o")
        argsProp.setProperty("hdfsoperation", hdfsOperation)
      }
      if (cmd.hasOption("hf")) {
        val hdfsFileName: String = cmd.getOptionValue("hf")
        argsProp.setProperty("hdfsfilename", hdfsFileName)
      }
      if (cmd.hasOption("lf")) {
        val localFileName: String = cmd.getOptionValue("lf")
        argsProp.setProperty("localfilename", localFileName)
      }
    } catch {
      case e: ParseException =>
        logger.info("Error parsing command-line arguments!")
        logger.info("Please, follow the instructions below:")
        val formatter: HelpFormatter = new HelpFormatter
        formatter.printHelp("webHDFS client", optionsSimple)
        System.exit(1)
    }
    return argsProp
  }
}
