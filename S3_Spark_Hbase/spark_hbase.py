from operator import sub
import findspark
import os

from sqlalchemy import distinct
findspark.init(os.environ.get('SPARK_HOME'))
import logging
import logging.config
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace
# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1 pyspark-shell"

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

    def transform_spark_df(self):
        """
            This method transform the data into meaningful informations
        """
        self.df.printSchema()
        # replacing the k,m,b with its value and casting the data type
        self.df= self.df.withColumn('follower_count', when(col('follower_count').like('%k'), regexp_replace('follower_count', 'k', '000'))\
            .when(col('follower_count').like('%M'),regexp_replace('follower_count', 'M', '000000'))\
            .when(col('follower_count').like('%B'),regexp_replace('follower_count', 'B', '000000000'))\
            .cast('int'))
        # Change Data type to int
        self.df = self.df.withColumn('downloaded', self.df['downloaded'].cast('int'))\
            .withColumn('index', self.df['index'].cast('int'))
        
        # replacing the false value with None
        self.df=self.df.replace({'User Info Error': None}, subset = ['follower_count']) \
                    .replace({'No description available Story format':None}, subset=['description'])\
                    .replace({'No Title Data Available':None}, subset=['title'])\
                    .replace({'Image src error.':None},subset=['image_src'])
        # only path should show in column
        self.df = self.df.withColumn('save_location',regexp_replace('save_location', 'Local save in ', ''))

        #replace  empty value with null
       
        self.df = self.df.select([when(col(cl)=='',None).otherwise(col(cl)).alias(cl) for cl in self.df.columns]) 
        
        # rename the column index because we are going to store in db 
        self.df = self.df.withColumnRenamed("index","indx")
        
        # desire the result
        self.df = self.df.select('indx',
                                'unique_id',
                                'title',
                                'description',
                                'category',
                                'follower_count',
                                'tag_list',
                                'is_image_or_video',
                                'image_src',
                                'downloaded',
                                'save_location')

        self.df = self.df.distinct()
        self.df.show(30)
    
    def run_spark_hbase(self):
        """ This method create spark session- read data from S3 -Transform data-store into HBASE """
        #logger.info('This is a info message in spark hbase')
        self.read_data_from_S3()
        self.transform_spark_df()

if __name__ == "__main__":

    #logging.config.fileConfig(fname='../logging.config', disable_existing_loggers=False)
    # Get the logger specified in the file
    #logger = logging.getLogger(__name__)
    # Run class with it's object
    spark_obj = Spark_Read_Write_Data()
    spark_obj.run_spark_hbase()
