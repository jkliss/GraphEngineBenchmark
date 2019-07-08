#!/usr/bin/sh
# Ubuntu 16.04
#Prequisites
sudo apt install -y libunwind8 g++ cmake libssl-dev git

#Dotnet Core
wget -q https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb

sudo add-apt-repository universe
sudo apt install -y apt-transport-https
sudo apt update
sudo apt install -y dotnet-sdk-2.2

#Git clone
rm -rf ./GraphEngine/

git clone https://github.com/Microsoft/GraphEngine.git
#git clone https://github.com/ToxicJojo/graph-engine-samples.git
git clone https://github.com/jkliss/GraphEngineBenchmark.git

#Update build.sh (reduces memory usage while compiling though resticting opened threads to number of cores available)
sed -i 's/make -j/make -j `nproc`/g' ./GraphEngine/tools/build.sh

#Building
~/GraphEngine/tools/build.sh
