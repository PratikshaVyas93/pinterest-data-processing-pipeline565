import findspark
import os
findspark.init(os.environ.get('SPARK_HOME'))
import logging
import logging.config
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

#os.environ['PYSPARK_SUBMIT_ARGS']='--packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.100 pyspark-shell'
# '--packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.100,org.apache.hadoop:hadoop-common:3.3.1'
class Spark_Read_Write_Data():
    """
        This class is mainly used for receiving data from S3 nucket and store it in spark data frame locally and 
        perform transformation get some meaning full informatiom and store it on HBASE database.
    """

    def __init__(self) -> None:
        # Creating our Spark configuration
       # Creating our Spark configuration
        conf = SparkConf() \
            .setAppName('S3toSpark') \
            .setMaster('local[*]')

        sc=SparkContext(conf=conf)

        # Configure the setting to read from the S3 bucket
        accessKeyId= os.environ.get('ACCESS_KEY')
        secretAccessKey=os.environ.get('SECRET_KEY')
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', accessKeyId)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

       # Create our Spark session
        self.spark_sn=SparkSession(sc)

    def read_data_from_S3(self):
        """
            This method reads data from s3
        """    
        self.df = self.spark_sn.read.json("s3a://pinterest-data-pipeline/pinterestdata/*.json")
        print(self.df, "this is data frame")

    def run_spark_hbase(self):
        """ This method create spark session- read data from S3 -Transform data-store into HBASE """
        #logger.info('This is a info message in spark hbase')
        self.read_data_from_S3()

if __name__ == "__main__":

    #logging.config.fileConfig(fname='../logging.config', disable_existing_loggers=False)
    # Get the logger specified in the file
    #logger = logging.getLogger(__name__)
    # Run class with it's object
    spark_obj = Spark_Read_Write_Data()
    spark_obj.run_spark_hbase()
