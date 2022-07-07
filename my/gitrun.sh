#!/bin/sh

export LC_ALL="zh_CN.UTF-8"

rm -fr redkale-plugins

rm -fr src

git clone https://github.com/redkale/redkale-plugins.git

cp -fr redkale-plugins/src ./

mvn clean
mvn deploy
