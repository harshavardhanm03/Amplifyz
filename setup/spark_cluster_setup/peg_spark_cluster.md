#Spark_Clutser Setup

Sparkcluster was set up using peg.


https://github.com/InsightDataScience/pegasus

#Commands for spark-cluster setup on VM
$ git clone https://github.com/InsightDataScience/pegasus.git
$ pip install awscli

add the following to your ~/.bash_profile.

export AWS_ACCESS_KEY_ID=XXXX
export AWS_SECRET_ACCESS_KEY=XXXX
export AWS_DEFAULT_REGION=XX-XXXX-X
export REM_USER=ubuntu
export PEGASUS_HOME=<path-to-pegasus>
export PATH=$PEGASUS_HOME:$PATH


Source BashProfile

$ source ~/.bash_profile


#Verify installation

$peg config

$peg aws vpcs

$peg aws subnets

$peg aws security-groups


#Launching th spark 
$ peg up master.yml
$peg up slaves.yml


#Fetching cluster-name

peg fetch spark-cluster



