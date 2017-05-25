package cn.hackershell.yarn.client.command;

import cn.hackershell.yarn.generated.AmServer;
import cn.hackershell.yarn.client.SubmitClient;
import cn.hackershell.yarn.client.ThriftClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by tangshangwen on 17-3-17.
 */

public class ThriftClientCommand implements Command {

    private COMMAND command;
    private Map<String, String> config;
    private AmServer.Client thriftClient;

    public ThriftClientCommand(Map<String, String> config, COMMAND command) {
        this.command = command;
        this.config = checkNotNull(config);
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        switch (command) {
            case ADD_WORKERS:
                options.addOption("num", true, "add number");
                break;
            case RESERVE_WORKERS:
                options.addOption("num", true, "reserve number");
                break;
            case START_AM_SERVER:
            case STOP_AM_SERVER:
            case START_WORKERS:
            case STOP_WORKERS:
            case SHUTDOWN:
            case GET_WORKERS_NUM:
            case GET_ALL_WORKERS:
                break;
        }
        options.addOption("cluster", true, "(Required) The standalone clusters Name");
        return options;
    }

    @Override
    public String getDescription() {
        String desc = null;
        switch (command) {
            case START_AM_SERVER:
                desc = "standalone-yarn startAmServer";
                break;
            case STOP_AM_SERVER:
                desc = "standalone-yarn stopAmServer";
                break;
            case START_WORKERS:
                desc = "standalone-yarn startWorkers";
                break;
            case STOP_WORKERS:
                desc = "standalone-yarn stopWorkers";
                break;
            case SHUTDOWN:
                desc = "standalone-yarn shutdown";
                break;
            case ADD_WORKERS:
                desc = "standalone-yarn addWorkers";
                break;
            case RESERVE_WORKERS:
                desc = "standalone-yarn reserveWorkers";
                break;
            case GET_WORKERS_NUM:
                desc = "standalone-yarn getWorkersNum";
                break;
            case GET_ALL_WORKERS:
                desc = "standalone-yarn getAllWorkers";
                break;
        }
        return desc;
    }

    @Override
    public void process(CommandLine commandLine) throws Exception {
        switch (command) {
            case START_AM_SERVER:
                SubmitClient submitClient = new SubmitClient(config);
                submitClient.submitApplication();
                System.out.println("star am server success");
                break;
            case STOP_AM_SERVER:
                thriftClient = ThriftClient.create();
                thriftClient.stopAmServer();
                System.out.println("stop am server success");
                break;
            case START_WORKERS:
                thriftClient = ThriftClient.create();
                thriftClient.startWorkers();
                System.out.println("start workers success");
                break;
            case STOP_WORKERS:
                thriftClient = ThriftClient.create();
                thriftClient.stopWorkers();
                System.out.println("stop workers success");
                break;
            case SHUTDOWN:
                thriftClient = ThriftClient.create();
                thriftClient.shutdown();
                System.out.println("shutdown success");
                break;
            case ADD_WORKERS:
                thriftClient = ThriftClient.create();
                int num = Integer.valueOf(commandLine.getOptionValue("num"));
                System.out.println("--------" + commandLine);
                thriftClient.addWorkers(num);
                System.out.println("add " + num + " workers");
                break;
            case RESERVE_WORKERS:
                thriftClient = ThriftClient.create();
                int number = Integer.valueOf(commandLine.getOptionValue("num"));
                thriftClient.reserveWorkers(number);
                System.out.println("reserve " + number + " workers");
                break;
            case GET_WORKERS_NUM:
                thriftClient = ThriftClient.create();
                System.out.println("workers num");
                System.out.println("total workers num:" + thriftClient.getWorkersNum());
                break;
            case GET_ALL_WORKERS:
                thriftClient = ThriftClient.create();
                Map<String, String> workers = checkNotNull(thriftClient.getAllWorkers());
                for (Map.Entry<String, String> worker : workers.entrySet()) {
                    System.out.println(worker.getKey() + ":" + worker.getValue());
                }
                break;
            default:
                System.out.println("sorry.I don't know what to do");
                break;

        }
    }
}
