#! /bin/bash

# manually - start
##################

# uncomment wheel group from sudeors and comment sudo to all users
cp /etc/sudoers /etc/sudoers.gen
sed -i '/^# %wheel.*NOPASSWD: ALL$/s/^# //' /etc/sudoers
sed -i '/^metang.*ALL$/s/^/# /' /etc/sudoers
sed -i '/^ALL.*ALL$/s/^/# /' /etc/sudoers
################
# manually - end

# switch to root
sudo su -

# add user onto (with home directory)
useradd -m onto
# change password for user
echo "ontology" | passwd --stdin onto
# add user to wheel group
usermod -aG wheel onto

# change PS1
cp /etc/profile /etc/profile.gen


yum -y update

curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"

python get-pip.py
yum install lrzsz -y
yum install vim -y
yum install screen -y

# install Python2.7
yum groupinstall "Development tools" -y
yum install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel -y

yum install java-1.8.0-openjdk-devel

# configure ulimit to 40,000 for neo4j user
cp -a /etc/security/limits.conf /etc/security/limits.conf.gen
sed -i '/^# End of file$/i \
neo4j\t\t\-\tnofile\t\t40000' /etc/security/limits.conf

curl -sL https://rpm.nodesource.com/setup_10.x | sudo bash -
cat <<EOF>  /etc/yum.repos.d/neo4j.repo
[neo4j]
name=Neo4j Yum Repo
baseurl=http://yum.neo4j.org/stable
enabled=1
gpgcheck=1
EOF


yum install neo4j
#dbms.allow_upgrade=true
#dbms.security.allow_csv_import_from_file_urls=true
#dbms.connectors.default_listen_address=0.0.0.0
yum install -y nodejs
npm install gulp -g

cd /tmp
wget http://debian.neo4j.org/neotechnology.gpg.key
rpm --import neotechnology.gpg.key

# install a package (change version if needed)
yum install neo4j -y

service neo4j start
chkconfig neo4j on
# make sure you enter the password in the web UI


yum install nginx -y
# install gunicorn
pip install gunicorn

mkdir /var/log/onto/
chown onto: /var/log/onto/

# install iPython
pip install ipython

# install BeautifulSoap
pip install beautifulsoup4

# install teradata ODBC
yum install ksh
yum install gcc
yum install make
ln -s /bin/ksh /usr/bin/ksh


pip install Flask
pip install flask-restful
pip install pyodbc
pip install py2neo
pip install requests
pip install gunicorn
pip install xmltodict
pip install xkcdpass



# install python-ldap
yum install python-devel -y
yum install openldap-devel -y
pip install python-ldap
