### Standalone on Yarn项目

编译执行执行脚本./bin/build.sh即可，就可以在目录build看到压缩包

提交时需要配置
```
yarn.standalone.master.address=spark://172.16.170.130:7077 #master的地址
yarn.standalone.hdfs.path=hdfs://ns1/tmp/yarnStandalone/ #压缩包的目录
yarn.standalone.appmaster.tar=standalone-yarn.zip #am的jar包名字
yarn.standalone.tar=standalone.tar #worker的jar包的名字
```
提交
```
standalone-yarn.sh startAmServer
```

增加worker
```
sh standalone-yarn.sh reserveWorkers -num 15
```
