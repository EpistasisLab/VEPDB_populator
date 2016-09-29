#install_cassandra.sh
#Commands to install Java 8 and DataStax Cassandra (with cqlsh)

#install java
sudo su
yum update -y
yum remove java-1.7.0-openjdk -y
yum install java-1.8.0 -y

#install cassandra
cd /etc/yum.repos.d/
touch datastax.repo
printf '%s\n%s\n%s\n%s\n%s\n' '[datastax]' 'name = DataStax Repo for Apache Cassandra' 'baseurl = http://rpm.datastax.com/community' 'enabled = 1' 'gpgcheck = 0' >> datastax.repo

#for a list of available versions, see http://rpm.datastax.com/community/noarch/
#Here I specify Cassandra v3.0.5, because existing VepDB images are using this version, and we need to use this exact version to avoid version difference when adding new nodes to an existing cluster. It’s certainly OK to update the version if we are building a brand new VepDB

yum install dsc30-3.0.5-1 -y
yum install cassandra30-tools-3.0.5-1 -y
pip install cqlsh

# check if cassandra is running: ps aux | grep cassandra
# if so, stop it: service cassandra stop
# in practice I never saw it running at this stage
cd /var/lib/cassandra/
rm -rf *
chmod 777 /var/lib/cassandra
cd /etc/cassandra/conf/
================================================
