package com.cargill.hdfs;

import com.google.gson.Gson;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.ProtocolException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/18/2020, Thu
 **/
public class HDFSConnection {
    protected static final Logger logger = LoggerFactory.getLogger(HDFSConnection.class);

    private String httpfsUrl = HDFSConnectionFactory.DEFAULT_PROTOCOL
            + HDFSConnectionFactory.DEFAULT_HOST + ":" + HDFSConnectionFactory.DEFAULT_PORT;
    private String principal = HDFSConnectionFactory.DEFAULT_USERNAME;
    private String password = HDFSConnectionFactory.DEFAULT_PASSWORD;
    private String cdpEnv = HDFSConnectionFactory.DEFAULT_CDPENV;

    public HDFSConnection() {
    }

    public HDFSConnection(String httpfsUrl, String cdpEnv, String principal, String password) {
        this.httpfsUrl = httpfsUrl;
        this.cdpEnv = cdpEnv;
        this.principal = principal;
        this.password = password;
    }

    private String responseMessage(HttpsURLConnection conn, boolean input) throws IOException {
        StringBuffer sb = new StringBuffer();
        if (input) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = null;

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            is.close();
        }
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("code", conn.getResponseCode());
        result.put("message", conn.getResponseMessage());
        result.put("type", conn.getContentType());
        result.put("data", sb);
        //
        // Convert a Map into JSON string.
        //
        Gson gson = new Gson();
        String json = gson.toJson(result);
        //logger.info("JSON Response = " + json);

        //
        // Convert JSON string back to Map.
        //
        // Type type = new TypeToken<Map<String, Object>>(){}.getType();
        // Map<String, Object> map = gson.fromJson(json, type);
        // for (String key : map.keySet()) {
        // System.out.println("map.get = " + map.get(key));
        // }

        return json;
    }

    public HttpsURLConnection getConnection(String webHDFSURL, String methodType) throws IOException {
        String httpsURL = webHDFSURL;
        String username = this.principal;
        String password = this.password;
        String userpass = username + ":" + password;

        String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));

        URL cdpUrl = new URL(httpsURL);
        HttpsURLConnection conn = (HttpsURLConnection) cdpUrl.openConnection();

        // Indicate that we want to write to the HTTP request body
        if (conn instanceof HttpsURLConnection) {
            conn.setRequestProperty("Authorization", basicAuth);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod(methodType);
            conn.setRequestProperty("Content-type", "text/plain");
        }
        logger.info("Establishing connection to webHDFS endpoint: "+this.httpfsUrl);
        return conn;
    }
    ///user/ps784744/test_dir/titanic.csv
    public void downloadFromHDFS(String hdfsFileName, String localFileName) throws IOException {
        //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=OPEN"
        String appendURL = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=OPEN",this.cdpEnv ,hdfsFileName);
        String webHDFSURL = this.httpfsUrl + appendURL;

        logger.info("\nSending 'GET' request to URL : " + webHDFSURL);
        HttpsURLConnection conn = this.getConnection(webHDFSURL, "GET");

        conn.connect();

        // Create Output local file stream to write the HDFS file content
        try{
            BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
            BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(localFileName));

            logger.info("Writing the file to local filesystem: " + localFileName);
            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        String responseJSON = responseMessage(conn, false);
        conn.disconnect();

        int responseCode = conn.getResponseCode();
        logger.info("Response Code : " + responseCode);
        logger.info("JSON Response = " + responseJSON.toString());

    }

    public void uploadFileToHDFS(String localFileName, String hdfsFileName) throws IOException {
        //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=CREATE&overwrite=true";
        String appendURL = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=CREATE&overwrite=true",this.cdpEnv ,hdfsFileName);
        String webHDFSURL = this.httpfsUrl + appendURL;

        HttpsURLConnection conn = this.getConnection(webHDFSURL, "PUT");
        logger.info("\nSending 'PUT' request to URL : " + webHDFSURL);

        conn.connect();

        // Create Output HDFS stream to write the local file content
        try{
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(localFileName));
            BufferedOutputStream fileOutputStream = new BufferedOutputStream(conn.getOutputStream());

            logger.info("Writing the file to HDFS: " + hdfsFileName);

            int bytesRead;
            byte dataBuffer[] = new byte[1024];
            // read byte by byte until end of stream
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        String responseJSON = responseMessage(conn, false);
        conn.disconnect();

        int responseCode = conn.getResponseCode();
        logger.info("Response Code : " + responseCode);
        logger.info("JSON Response = " + responseJSON.toString());

    }

    public void deleteFileOnHDFS(String hdfsFileName) throws IOException {
        //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=DELETE";
        String appendURL = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=DELETE",this.cdpEnv ,hdfsFileName);
        String webHDFSURL = this.httpfsUrl + appendURL;

        HttpsURLConnection conn = this.getConnection(webHDFSURL, "DELETE");
        logger.info("\nSending 'DELETE' request to URL : " + webHDFSURL);

        conn.connect();

        String responseJSON = responseMessage(conn, false);
        conn.disconnect();

        int responseCode = conn.getResponseCode();
        logger.info("Response Code : " + responseCode);
        logger.info("JSON Response = " + responseJSON.toString());

    }

    public void getDirectoryStatus(String hdfsFileName) throws IOException {
        //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir/titanic.csv?op=LISTSTATUS";
        String appendURL = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=LISTSTATUS",this.cdpEnv ,hdfsFileName);
        String webHDFSURL = this.httpfsUrl + appendURL;

        HttpsURLConnection conn = this.getConnection(webHDFSURL, "GET");
        logger.info("\nSending 'GET' request to URL : " + webHDFSURL);

        conn.connect();

        String responseJSON = responseMessage(conn, false);
        conn.disconnect();

        int responseCode = conn.getResponseCode();
        logger.info("Response Code : " + responseCode);
        logger.info("JSON Response = " + responseJSON.toString());

    }

    public void createDirectory(String hdfsFileName) throws IOException {
        //"https://drona-haproxy.cargill.com:8443/gateway/drona/webhdfs/v1/user/ps784744/test_dir?op=MKDIRS";
        String appendURL = MessageFormat.format("/gateway/{0}/webhdfs/v1{1}?op=MKDIRS",this.cdpEnv ,hdfsFileName);
        String webHDFSURL = this.httpfsUrl + appendURL;

        HttpsURLConnection conn = this.getConnection(webHDFSURL, "PUT");
        logger.info("\nSending 'PUT' request to URL : " + webHDFSURL);

        conn.connect();

        String responseJSON = responseMessage(conn, false);
        conn.disconnect();

        int responseCode = conn.getResponseCode();
        logger.info("Response Code : " + responseCode);
        logger.info("JSON Response = " + responseJSON.toString());

    }

}
