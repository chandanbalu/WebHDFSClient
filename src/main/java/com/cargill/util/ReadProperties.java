package com.cargill.util;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/18/2020, Thu
 **/
public class ReadProperties {
    /** Utility functions load the Twitter Properties file. */

    public Properties loadProperties(String resourcePath) throws Exception{
        /** Read the file and load it to the properties. */

        Properties props = new Properties();
        props.load(this.getClass().getResourceAsStream(resourcePath));
        return props;
    }
}
