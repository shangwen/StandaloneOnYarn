package com.jd.bdp.yarn.client;

import com.jd.bdp.yarn.client.command.COMMAND;
import com.jd.bdp.yarn.client.command.Command;
import com.jd.bdp.yarn.client.command.HelpCommand;
import com.jd.bdp.yarn.client.command.ThriftClientCommand;
import com.jd.bdp.yarn.utils.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tangshangwen on 17-3-17.
 */
public class StandaloneYarn {
    private static final Log LOG = LogFactory.getLog(StandaloneYarn.class);
    private Map<String, String> config;
    private HashMap<String, Command> commands;
    private HelpCommand help;

    private StandaloneYarn() {
        config = Config.create4Env();
        commands = new HashMap<>();
        help = new HelpCommand(commands);
    }

    public void execute(String[] args) throws Exception {
        commands.put("help", help);
        commands.put("startAmServer", new ThriftClientCommand(config, COMMAND.START_AM_SERVER));
        commands.put("stopAmServer", new ThriftClientCommand(config, COMMAND.STOP_AM_SERVER));
        commands.put("startWorkers", new ThriftClientCommand(config, COMMAND.START_WORKERS));
        commands.put("stopWorkers", new ThriftClientCommand(config, COMMAND.STOP_WORKERS));
        commands.put("shutdown", new ThriftClientCommand(config, COMMAND.SHUTDOWN));
        commands.put("addWorkers", new ThriftClientCommand(config, COMMAND.ADD_WORKERS));
        commands.put("reserveWorkers", new ThriftClientCommand(config, COMMAND.RESERVE_WORKERS));
        commands.put("getWorkersNum", new ThriftClientCommand(config, COMMAND.GET_WORKERS_NUM));

        String commandName = null;
        String[] commandArgs = null;
        if (args.length<1){
            commandName = "help";
            commandArgs = new String[0];
        }else {
            commandName = args[0];
            commandArgs = Arrays.copyOfRange(args,1,args.length);
        }

        Command command = commands.get(commandName);
        if (command == null){
            LOG.error("ERROR: "+ commandName + " is not a supoorted command.");
            //TODO print help info
            System.exit(1);
        }
        Options options = command.getOptions();
        if (!options.hasOption("h")){
            options.addOption("h","help",false,"print out a help message");
        }
        CommandLine commandLine = new GnuParser().parse(command.getOptions(),commandArgs);
        if (commandLine.hasOption("help")){
            //TODO print help info
            help.printHelpInfoFor(Arrays.asList(commandName));
        }else {
            command.process(commandLine);
        }
    }

    public static void main(String[] args) {
        StandaloneYarn standaloneYarn = new StandaloneYarn();
        try {
            standaloneYarn.execute(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
