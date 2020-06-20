package com.cargill.hdfsclient;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/17/2020, Wed
 **/

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSException;

public class SimpleHDFSClient {

    public static void main(String[] args) throws IOException, GSSException {

        // set kerberos host and realm
        System.setProperty("java.security.krb5.realm", "NA.CORP.CARGILL.COM");
        System.setProperty("java.security.krb5.kdc", "ladc1.la.corp.cargill.com");

        Configuration conf = new Configuration();

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        //hdfs://nameservice1
        conf.set("fs.defaultFS", "hdfs://drona-master3.cargill.com:8020");
        //conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        // hack for running locally with fake DNS records
        // set this to true if overriding the host name in /etc/hosts
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("hadoop.rpc.protection", "privacy");

        // server principal
        // the kerberos principle that the namenode is using
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@NA.CORP.CARGILL.COM");

        //SimpleHDFSClient.class.getResource("/ps784744.keytab").getFile()
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("ps784744@NA.CORP.CARGILL.COM", "C:\\Users\\c795701\\WebHDFSClient\\src\\main\\resources\\ps784744.keytab");

        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/user/ps784744/test_dir"), true);
        while(files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println(IOUtils.toString(fs.open(file.getPath())));
        }
    }
}

