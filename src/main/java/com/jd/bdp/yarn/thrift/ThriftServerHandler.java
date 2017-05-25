package com.jd.bdp.yarn.thrift;

import com.jd.bdp.yarn.client.RMClient;
import com.jd.bdp.yarn.generated.AmServer;
import com.jd.bdp.yarn.utils.StandaloneOnYarnCommonKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * Created by tangshangwen on 17-3-15.
 */
public class ThriftServerHandler implements AmServer.Iface, WorkerInfo {

    private static final Log LOG = LogFactory.getLog(ThriftServerHandler.class);
    private Configuration conf;
    private RMClient rmClient;
    /**
     * 目标Worker数量，同来和申请的数量做比较
     */
    private int currentContainerNum = 0;
    private boolean isRelaxLocality;
    private Priority workerPri;
    private Thread workerStatusThread;

    public ThriftServerHandler(Configuration conf) {
        this.conf = conf;
        this.rmClient = new RMClient(conf, this);
        this.isRelaxLocality = conf.getBoolean(
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_RELAX_LOCALITY, true);
        this.workerPri = Records.newRecord(Priority.class);
        this.workerPri.setPriority(
                conf.getInt(StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_PRIORITY,
                        StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_PRIORITY_DEFAULT));
        workerStatusThread = new WorkerStatusThread();
        workerStatusThread.start();
    }

    private String[] getWorkNodes() {
        String[] workers = conf.getStrings(
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKERS_KEY);
        return workers;
    }

    private String[] getRacks() {
        return null;
    }

    @Override
    public synchronized void addWorkers(int number) throws TException {
        LOG.info("Server add workers=" + number);
        this.currentContainerNum += number;
        createWorker(number);
    }

    private synchronized void createWorker(int numWorker) {
        int cores = conf.getInt(StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_CORE,
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_CORE_DEFAULT);
        int mem = conf.getInt(StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_MEM,
                StandaloneOnYarnCommonKeys.YARN_STANDALONE_WORKER_MEM_DEFAULT);

        Resource workerResource = Resource.newInstance(mem * 1024, cores);
        String[] nodes = getWorkNodes();
        String[] racks = getRacks();

        LOG.info("setting worker cores=" + cores + " mem=" + mem + " numWorker=" + numWorker);
        for (int i = 0; i < numWorker ; i++) {
            rmClient.addContainerRequest(
                    rmClient.buildContainer(workerResource, nodes, racks, workerPri, isRelaxLocality));
        }
    }

    @Override
    public void reserveWorkers(int number) throws TException {
        LOG.info("Server reserve workers=" + number + " current workers=" + rmClient.getCurrentWorksSize());
        this.currentContainerNum = number;
        if (number > rmClient.getCurrentWorksSize()) {
            int newWorkerNum = number - rmClient.getCurrentWorksSize();
            createWorker(newWorkerNum);
        } else if (number < rmClient.getCurrentWorksSize()) {
            int stopWorkers = rmClient.getCurrentWorksSize() - number;
            rmClient.stopWorkers(stopWorkers);
        }
    }

    @Override
    public int getWorkersNum() throws TException {
        return 0;
    }

    @Override
    public int currentContainerNum() throws TException {
        return 0;
    }

    @Override
    public String getContainerInfo() throws TException {
        return null;
    }

    @Override
    public void startAmServer() throws TException {

    }

    @Override
    public void stopAmServer() throws TException {

    }

    @Override
    public void startWorkers() throws TException {

    }

    @Override
    public void stopWorkers() throws TException {
       rmClient.stopAllWorkers();
    }

    @Override
    public void shutdown() throws TException {

    }

    @Override
    public Map<String, String> getAllWorkers() throws TException {
        return null;
    }

    @Override
    public void setAmServerConf(Map<String, String> conf) throws TException {

    }

    @Override
    public void checkContainerState() throws TException {

    }

    @Override
    public int getNumWorkers() {
        return currentContainerNum;
    }

    private class WorkerStatusThread extends Thread {
        public WorkerStatusThread() {
            super(WorkerStatusThread.class.getName());
        }

        @Override
        public void run() {
            while(true) {
                try {
                    if (rmClient.getCurrentWorksSize() < currentContainerNum) {
                        int num = currentContainerNum - rmClient.getCurrentWorksSize();
                        LOG.info("In WorkerStatusThread fixed TargetWorksNum=" + currentContainerNum
                                + " CurrentWorkNum=" + rmClient.getCurrentWorksSize());
                        createWorker(num);
                    }
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
