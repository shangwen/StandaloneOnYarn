#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
export STANDALONE_ON_YARN_HOME="$(cd "$SCRIPT_DIR/.."; pwd)"

cd $STANDALONE_ON_YARN_HOME

mvn clean package
rm -rf ./build
mkdir -p ./build/standalone-yarn-0.1/
cp -a ./target/standalone-on-yarn-1.0-SNAPSHOT-jar-with-dependencies.jar ./build/standalone-yarn-0.1/standalone-yarn-0.1.jar
cp -a ./bin ./build/standalone-yarn-0.1/
cp -a ./etc ./build/standalone-yarn-0.1/
cd ./build/standalone-yarn-0.1/
zip -r ../standalone-yarn.zip *




