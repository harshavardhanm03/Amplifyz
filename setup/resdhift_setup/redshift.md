#setting up redshift for writing data from s3 to redshift

Node tyep: dc2 large
nodes: 2
cluster identifer:red-shift-cluster-1
databse port :5439
master-user-name:******
password:******

#Advance setting of VPC
ClusterName:amazon-reviews
Databasename:amazon
port:5439
Master-username:
master-password:

select default VPC 
Select default VPC security group
Attach IAM roles for IAM-harsha


Tables will be created in public
stg_load_errors need to be checked if there is any errror during the loading of data.

#Documentation followed for copying data from s3 to redshift
https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html