package com.cargill.hdfs;

import com.cargill.hdfs.master.KerberosWebHDFSConnection;
import com.cargill.hdfs.master.PseudoWebHDFSConnection;
import com.cargill.hdfs.master.WebHDFSConnection;
import com.cargill.hdfs.master.WebHDFSConnectionFactory;
import com.cargill.util.ReadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/18/2020, Thu
 **/
public class HDFSConnectionFactory {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /** The default CDP to connect to */
    public static final String DEFAULT_CDPENV = "drona";

    /** The default host to connect to */
    public static final String DEFAULT_HOST = "drona-haproxy.cargill.com";

    /** The default port */
    public static final String DEFAULT_PORT = "8443";

    /** The default username */
    public static final String DEFAULT_USERNAME = "ps784744";

    /** The default username */
    public static final String DEFAULT_PASSWORD = "password";

    public static final String DEFAULT_PROTOCOL = "https://";

    public static enum AuthenticationType {
        KERBEROS, SIMPLE
    }

    private String 	cdpEnv 	= 	DEFAULT_CDPENV;
    private String 	host 	= 	DEFAULT_HOST;
    private String 	port	=	DEFAULT_PORT;
    private String 	username=	DEFAULT_USERNAME;
    private String 	password=	DEFAULT_PASSWORD;
    private String 	authenticationType	= WebHDFSConnectionFactory.AuthenticationType.KERBEROS.name();
    private WebHDFSConnection webHDFSConnection;

    public HDFSConnectionFactory() throws Exception {
        String resourcePath = "/cdp.properties";

        logger.info("Reading the CDP properties file for environment setup from " + this.getClass().getResource(resourcePath).toString());

        Properties props = new ReadProperties().loadProperties(resourcePath);
        this.cdpEnv = props.getProperty("webhdfs.cdpenv");
        this.host = props.getProperty("webhdfs.host");
        this.port = props.getProperty("webhdfs.port");
        this.username = props.getProperty("webhdfs.username");
        this.password = props.getProperty("webhdfs.password");
        this.authenticationType = AuthenticationType.SIMPLE.name();
    }

    public HDFSConnectionFactory(String cdpEnv, String host, String port , String username, String password, String authType) {
        this.cdpEnv = cdpEnv;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.authenticationType = authType;
    }

    public HDFSConnection getConnection() {
        String httpfsUrl = HDFSConnectionFactory.DEFAULT_PROTOCOL
                + this.host + ":" + this.port;

        HDFSConnection hdfsConnection = new HDFSConnection(httpfsUrl, this.cdpEnv, this.username, this.password);
        return hdfsConnection;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAuthenticationType() {
        return authenticationType;
    }

    public void setAuthenticationType(String authenticationType) {
        this.authenticationType = authenticationType;
    }
}
