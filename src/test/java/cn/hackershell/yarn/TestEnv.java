package cn.hackershell.yarn;


import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Created by tangshangwen on 17-3-16.
 */
public class TestEnv {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("yarn.standalone.env.SPARK_WORKER_DIR","AAA");
        conf.set("yarn.standalone.env.A","AAA");
        conf.set("yarn.standalone.env.B","BBB");
        conf.set("yarn.standalone.env.C","CCC");
        conf.set("yarn.standalone.yz","zzz");
        Map<String, String> map = conf.getValByRegex("yarn.standalone.env*");
        for (String str : map.keySet()) {
            System.out.println("str:" + str + " value:" + map.get(str));
        }
    }
}
