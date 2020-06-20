package com.cargill.util;

import com.cargill.hdfs.HDFSConnection;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/19/2020, Fri
 **/
public class ArgumentParser {
    protected static final Logger logger = LoggerFactory.getLogger(HDFSConnection.class);
    String argsConsole[],argsSimple[];
    Options optionsConsole = new Options();
    Options optionsSimple = new Options();

    public ArgumentParser(String optionType, String args[]){
        if(optionType == "Console"){
            this.argsConsole = args;
        } else if(optionType == "Simple"){
            this.argsSimple = args;
        } else {
            logger.info("Error initializing the ArgumentParser.");
            System.exit(1);
        }


    }

    public void parserError(String optionType){
        logger.info("Error parsing command-line arguments!");
        logger.info("Please, follow the instructions below:");
        HelpFormatter formatter = new HelpFormatter();

        if(optionType == "Console"){
            formatter.printHelp( "webHDFS client", optionsConsole );
        } else if(optionType == "Simple"){
            formatter.printHelp( "webHDFS client", optionsSimple );
        } else {
            logger.info("Error initializing the ArgumentParser.");
            System.exit(1);
        }
        System.exit(1);
    }
    public Properties getConsoleArgsProperties() throws ParseException {

        Option host = new Option("h", "host", true, "Hostname ([REQUIRED] or use --host)");
        host.setRequired(true);
        optionsConsole.addOption(host);

        Option port = new Option("n", "port", true, "Portnumber ([REQUIRED] or use --port)");
        port.setRequired(true);
        optionsConsole.addOption(port);

        Option username = new Option("u", "username", true, "Username ([REQUIRED] or use --username)");
        username.setRequired(true);
        optionsConsole.addOption(username);

        Option password = new Option("p", "password", true, "Hostname ([REQUIRED] or use --password)");
        password.setRequired(true);
        optionsConsole.addOption(password);

        Option hdfsfile = new Option("hf", "hdfsfile", true, "HDFS Filename ([OPTIONAL] or use --hdfsfile)");
        hdfsfile.setRequired(false);
        optionsConsole.addOption(hdfsfile);

        Option localfile = new Option("lf", "localfile", true, "Local Filename ([OPTIONAL] or use --localfile)");
        localfile.setRequired(false);
        optionsConsole.addOption(localfile);

        CommandLineParser parser = new DefaultParser();
        Properties argsProp = new Properties();

        try {
            CommandLine cmd = parser.parse(optionsConsole, argsConsole);
            if (cmd.hasOption("h")) {
                String hostName = cmd.getOptionValue("h");
                argsProp.setProperty("host", hostName);
            }
            if (cmd.hasOption("n")) {
                String portNumber = cmd.getOptionValue("n");
                argsProp.setProperty("port", portNumber);
            }
            if (cmd.hasOption("u")) {
                String userName = cmd.getOptionValue("u");
                argsProp.setProperty("username", userName);
            }
            if (cmd.hasOption("p")) {
                String passWord = cmd.getOptionValue("p");
                argsProp.setProperty("password", passWord);
            }
            if (cmd.hasOption("hf")) {
                String hdfsFile = cmd.getOptionValue("hf");
                argsProp.setProperty("hdfsfile", hdfsFile);
            }
            if (cmd.hasOption("lf")) {
                String localFile = cmd.getOptionValue("lf");
                argsProp.setProperty("localfile", localFile);
            }

        } catch (ParseException e) {
            logger.info("Error parsing command-line arguments!");
            logger.info("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "webHDFS client", optionsConsole );
            System.exit(1);
        }

        return argsProp;
    }

    public Properties getSimpleArgsProperties() throws ParseException {

        Option hdfsoperation = new Option("o", "operation", true, "HDFS Operation to be Performed ([REQUIRED] or use --operation)");
        hdfsoperation.setRequired(true);
        optionsSimple.addOption(hdfsoperation);

        Option hdfsfile = new Option("hf", "hdfsfile", true, "HDFS Filename ([OPTIONAL] or use --hdfsfile)");
        hdfsfile.setRequired(false);
        optionsSimple.addOption(hdfsfile);

        Option localfile = new Option("lf", "localfile", true, "Local Filename ([OPTIONAL] or use --localfile)");
        localfile.setRequired(false);
        optionsSimple.addOption(localfile);

        CommandLineParser parser = new DefaultParser();
        Properties argsProp = new Properties();

        try {
            CommandLine cmd = parser.parse(optionsSimple, argsSimple);
            if (cmd.hasOption("o")) {
                String hdfsOperation = cmd.getOptionValue("o");
                argsProp.setProperty("hdfsoperation", hdfsOperation);
            }
            if (cmd.hasOption("hf")) {
                String hdfsFileName = cmd.getOptionValue("hf");
                argsProp.setProperty("hdfsfilename", hdfsFileName);
            }
            if (cmd.hasOption("lf")) {
                String localFileName = cmd.getOptionValue("lf");
                argsProp.setProperty("localfilename", localFileName);
            }

        } catch (ParseException e) {
            logger.info("Error parsing command-line arguments!");
            logger.info("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "webHDFS client", optionsSimple );
            System.exit(1);
        }
        return argsProp;
    }
}
