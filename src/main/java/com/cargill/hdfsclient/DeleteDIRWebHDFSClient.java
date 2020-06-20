package com.cargill.hdfsclient;

import com.cargill.hdfs.HDFSConnection;
import com.cargill.util.ArgumentParser;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.URL;
import java.util.Base64;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/19/2020, Fri
 **/
public class DeleteDIRWebHDFSClient {
    protected static final Logger logger = LoggerFactory.getLogger(HDFSConnection.class);

    public static void main(String args[]) throws IOException, ParseException {

        ArgumentParser argParser = new ArgumentParser("Console", args);
        Properties argsProp = argParser.getConsoleArgsProperties();

        final String DEFAULT_PROTOCOL = "https://";
        final String DEFAULT_CDPENV = "drona";
        String host = argsProp.getProperty("host");
        String port = argsProp.getProperty("port");
        String username = argsProp.getProperty("username");
        String password = argsProp.getProperty("password");
        String hdfsFileName = argsProp.getProperty("hdfsfile");

        if(host==null || port==null || username==null || password==null || hdfsFileName==null){
            argParser.parserError("Console");
        }

        //https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir?op=LISTSTATUS
        String httpsURL = DEFAULT_PROTOCOL + host + ":" + port + "/gateway/" + DEFAULT_CDPENV
                + "/webhdfs/v1" + hdfsFileName + "?op=DELETE";

        String userpass = username + ":" + password;

        String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
        URL myUrl = new URL(httpsURL);
        HttpsURLConnection conn = (HttpsURLConnection)myUrl.openConnection();

        logger.info("Establishing connection to webHDFS endpoint: " + httpsURL);

        // Indicate that we want to write to the HTTP request body
        if (conn instanceof HttpsURLConnection) {
            conn.setRequestProperty("Authorization", basicAuth);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod("DELETE");
            conn.setRequestProperty("Content-type", "text/plain");
            conn.connect();
        }

        StringBuffer sb = new StringBuffer();
        if (false) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = null;

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            is.close();
        }

        conn.disconnect();

        int responseCode = conn.getResponseCode();
        logger.info("Response Code : " + responseCode);

    }
}
