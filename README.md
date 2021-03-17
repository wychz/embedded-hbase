# embedded-hbase
An embedded hbase which supports newer version of hbase 

This repository referred to kiji's project https://github.com/kijiproject/fake-hbase
embeddedhbase updates the supported hbase version to hbase-shaded-testing-util 2.3.0, Modifies the implementation of the methods to adjust to newer version.

介绍：
这是一个基于内存的HBase实现。不需要安装任何额外的环境，即可使用HBase的API进行相关操作，可以用于测试。
对众多常用方法进行了版本更新，适配hbase-shaded-testing-util 2.3.0版本。

使用方式：
clone本仓库，编译导入本地仓库。
项目pom引用该jar包即可。
