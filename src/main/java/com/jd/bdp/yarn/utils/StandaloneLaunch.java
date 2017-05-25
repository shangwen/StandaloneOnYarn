package com.jd.bdp.yarn.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tangshangwen on 17-3-16.
 */
abstract class StandaloneLaunch {
    private static final Log LOG = LogFactory.getLog(SparkLaunch.class);
    private Configuration conf;

    public StandaloneLaunch(Configuration conf) {
        this.conf = conf;
    }

    abstract ContainerLaunchContext getLanchContext() throws Exception;

    public Map<String, String> getContainerEnv() {
        Map<String, String> envMap = new HashMap<>();
        Map<String, String> valMap = conf.getValByRegex("yarn.standalone.env*");
        LOG.info("setting worker env...");
        for (String key : valMap.keySet()) {
            String[] arrays = key.split("\\.");
            LOG.info("setting " + arrays[arrays.length -1] + "=" + valMap.get(key));
            envMap.put(arrays[arrays.length -1], valMap.get(key));
        }
        return envMap;
    }
}
