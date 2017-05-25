package com.jd.bdp.yarn.client.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by qg_xiaoguang on 16/2/23.
 */
public class HelpCommand implements Command {

    private HashMap<String, Command> _commands;

    public HelpCommand(HashMap<String, Command> commands){
        this._commands = commands;
    }

    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public String getDescription() {
        return "standalone-yarn help";
    }

    @Override
    public void process(CommandLine commandLine) throws Exception {
        printHelpInfoFor(commandLine.getArgList());
    }

    public void printHelpInfoFor(Collection<String> args){
        if (args == null || args.size() < 1){
            args = _commands.keySet();
        }
        HelpFormatter formatter = new HelpFormatter();
        for (String commandName : args) {
            Command clientCommand = _commands.get(commandName);
            if (clientCommand!=null) {
                formatter.printHelp(commandName,clientCommand.getDescription(),clientCommand.getOptions(),null);
            } else {
                System.err.println("ERROR: " + clientCommand + " is not a supported command.");
            }
        }

    }
}
