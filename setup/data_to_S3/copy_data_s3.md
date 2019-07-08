#Dataset up 
Explains about fecthing data into s3 bucket 

Configure awscli using secret acces and keys
aws configure
aws s3 ls s3://amazon-reviews-pds/  
aws s3 sync s3://SOURCE_BUCKET_NAME s3://NEW_BUCKET_NAME  can be used

#Attach s3 bucket read and write policies for Spark Master and Salves EC2 instances


#s3a/s3n/s3 
For reading data of s3  in Spark see the properties of /usr/local/hadoop/etc/hadoop/
vi core-site.xml and use respective file systems


 




