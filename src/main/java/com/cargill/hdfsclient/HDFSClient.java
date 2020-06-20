package com.cargill.hdfsclient;

import com.cargill.hdfs.HDFSConnection;
import com.cargill.hdfs.HDFSConnectionFactory;
import com.cargill.util.ArgumentParser;
import com.cargill.util.ReadProperties;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/18/2020, Thu
 **/
public class HDFSClient {

    public static void main(String args[]) throws Exception {
        final Logger logger = LoggerFactory.getLogger(HDFSConnection.class);

        // Read the arguments using Parser
        ArgumentParser argParser = new ArgumentParser("Simple",args);
        Properties argsProp = argParser.getSimpleArgsProperties();

        String hdfsOperation = argsProp.getProperty("hdfsoperation");
        String hdfsFileName = argsProp.getProperty("hdfsfilename");
        String localFileName = argsProp.getProperty("localfilename");

        if(hdfsOperation==null){
            argParser.parserError("Simple");
        }
        if(hdfsOperation.equals("download") || hdfsOperation.equals("upload")){
            if(hdfsFileName==null || localFileName==null) argParser.parserError("Simple");
        }
        if(hdfsOperation.equals("create") || hdfsOperation.equals("list") || hdfsOperation.equals("delete"))
            if (hdfsFileName == null) argParser.parserError("Simple");


        /** Using the Factory build the hdfsConnection object */
        HDFSConnectionFactory hdfsConnectionFactory = new HDFSConnectionFactory();
        HDFSConnection hdfsConnection = hdfsConnectionFactory.getConnection();

        switch (hdfsOperation) {
            case "upload":
                hdfsConnection.uploadFileToHDFS(localFileName,hdfsFileName);
                break;
            case "download":
                hdfsConnection.downloadFromHDFS(hdfsFileName, localFileName);
                break;
            case "create":
                hdfsConnection.createDirectory(hdfsFileName);
                break;
            case "list":
                hdfsConnection.getDirectoryStatus(hdfsFileName);
                break;
            case "delete":
                hdfsConnection.deleteFileOnHDFS(hdfsFileName);
                break;
            default:
                logger.info("Invalid or Unsupported HDFS operation");
        }
    }
}
