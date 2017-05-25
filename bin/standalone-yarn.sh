#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
export STANDALONE_ON_YARN_HOME="$(cd "$SCRIPT_DIR/.."; pwd)"
if [ $1 = "startAmServer" ] ; then
echo $STANDALONE_ON_YARN_HOME/lib/standalone-yarn-0.1.jar
      hadoop jar $STANDALONE_ON_YARN_HOME/standalone-yarn-0.1.jar  com.jd.bdp.yarn.client.StandaloneYarn startAmServer
else
      hadoop jar $STANDALONE_ON_YARN_HOME/standalone-yarn-0.1.jar  com.jd.bdp.yarn.client.StandaloneYarn $1 $2 $3
fi
