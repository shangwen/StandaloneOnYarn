package cn.hackershell.yarn.client.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Created by tangshangwen on 17-3-17.
 */
public interface Command {

    public Options getOptions();

    public String getDescription();

    public void process(CommandLine commandLine) throws Exception;
}
