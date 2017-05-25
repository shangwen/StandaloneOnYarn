package com.jd.bdp.yarn.extend;

import com.jd.bdp.yarn.utils.StandaloneOnYarnCommonKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by tangshangwen on 17-3-17.
 */
public class ExtendServerFactory {
    private static final Log LOG = LogFactory.getLog(ExtendServerFactory.class);

    private ExtendServerFactory() {
    }

    public static ExtendServer create(Configuration conf) {
        Class extendServerClass =
                conf.getClass(StandaloneOnYarnCommonKeys.YARN_STANDALONE_EXTEND_SERVER,
                        HdfsExtendServerImpl.class);
        ExtendServer extendServer = null;
        try {
            LOG.info("Create " + extendServerClass.getCanonicalName() + " ...");
            Constructor constructor = extendServerClass.getConstructor(Configuration.class);
            extendServer = (ExtendServer) constructor.newInstance(conf);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return extendServer;
    }
}
