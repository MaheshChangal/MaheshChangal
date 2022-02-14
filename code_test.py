# Libraries used  
import random
import hashlib
import json
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql.functions import to_date,col,udf
import boto3
from datetime import date



rand_int = random.randint(0,100000000)
sc = SparkSession.builder.appName('App'+str(rand_int)).getOrCreate()
sc.sparkContext.addPyFile("/usr/lib/spark/jars/delta-core_2.12-0.8.0.jar")

from delta import *
from delta.tables import *

class Configuration:
    def __init__(self,spark_config_loc,datasetName):

        self.s3 = boto3.resource('s3')
        self.datasetname = datasetName

        #set spark configuration
        self.setSparkConfig(spark_config_loc)

        # Extracting information from app_config file
        self.conf = self.fetchConfig()

        self.raw_bucket = self.conf["raw-bucket"]
        self.staging_bucket = self.conf["staging-bucket"]

        # Maskable columns of dataset
        self.maskData = self.conf['mask-'+datasetName]
        self.df_maskColumns = self.maskData['masking-cols']

        #primary key values
        self.pii_cols = self.conf[self.datasetname+"-pii-cols"]

        #Data locations of the data residing in RawZone
        self.df_rawzone_src = self.maskData['source']['data-location']

        # Datatype conversions 
        self.df_datatypes = self.maskData['datatype-update-cols']

        self.staging_zone_dest = self.maskData['destination']['data-location']
        #partition columns
        self.partition_cols = self.maskData["partition-cols"]

    
    # Sets spark configs from location fetched from livy call
    def setSparkConfig(self,location):
  
        obj = self.s3.Object('landingzone5077', location)
        body = obj.get()['Body'].read()
        json_raw = json.loads(body)
        spark_properties = json_raw['Properties']
        lst = []
        
        for i in spark_properties:
            lst.append((i,spark_properties[i]))
            
        conf = sc.sparkContext._conf.setAll(lst)
        conf = sc._jsc.hadoopConfiguration().set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false')

    # fetchConfig is used to get app_config file from s3 bucket   
    def fetchConfig(self):
        path = sc.sparkContext._conf.get('spark.app_config')
        obj = self.s3.Object('landingzone5077', path)
        body = obj.get()['Body'].read()
        confData = json.loads(body)
        
        return confData


   

class TransformData:

    #convertDatatype is used to covert the datatype of a column
    def convertDatatype(self,conversion_lst,df):
        for item in conversion_lst:
            datatype = re.findall('([\w]*)(?:Type)',item.split(':')[1])[0].lower()
            column = item.split(':')[0]
            df = df.withColumn(column,df[column].cast(datatype))
        return df

    # moveFiles is used to transport files around various s3 buckets
    def moveFiles(self,df,file_format,destination):
        df.write.format(file_format).save(destination)
    
    def moveFilesWithPartition(self,df,file_format,col_name,destination):
        df.write.partitionBy(col_name).mode("append").format(file_format).save(destination)
    
    def hashing(self,value):
        sha_value = hashlib.sha256(value.encode()).hexdigest()
        return sha_value
    
    # maskColumns is used to encrypt the columns of a specific data frame
    def maskColumns(self,col_name,df):
        spark_udf = udf(self.hashing, StringType())
        df = df.withColumn('masked_'+col_name,spark_udf(col_name))
        return df
        
    def convertDecimals(self,df,col_name,scale):
        df = df.withColumn(col_name,df[col_name].cast(DecimalType(scale=scale)))
        return df
    
    
    
class Add_to_delta:

    def __init__(self,df,dataset,spark_conf_loc):
         self.df_source = df   
         self.dataset = dataset
         self.conf_obj = Configuration(spark_conf_loc,dataset)
    
    def setup(self):
        try:
            targetTable = DeltaTable.forPath(sc,"s3://stagingzone5077/Delta/"+self.dataset+"/")
        except:
            df_source = self.df_source.withColumn('active_flag',lit('Y'))
            df_source = df_source.withColumn('begin_date',lit(date.today()))
            df_source = df_source.withColumn('end_date',lit('null'))
            
            df_source.write.format('delta').mode('overwrite').save("s3://stagingzone5077/"+"Delta/"+self.dataset+"/")

            targetTable = DeltaTable.forPath(sc,"s3://stagingzone5077/Delta/"+self.dataset+"/")

        df_target = targetTable.toDF()
        return df_target
    
    def process(self):

        df_target = self.setup()
        primary_cols = self.conf_obj.pii_cols

        conditions = [(df_target.active_flag=="Y")]
        for col in primary_cols:
            conditions.append((df_target[col]==self.df_source[col]))
        
        fields_to_select = []
        for i in primary_cols:
            fields_to_select.append(self.df_source[i])
            fields_to_select.append(self.df_source["masked_"+i])
        for i in primary_cols:
            fields_to_select.append(df_target[i].alias("Target_"+i))
            fields_to_select.append(df_target["masked_"+i].alias("Target_"+"masked_"+i))
        

        joinDF = self.df_source.join(df_target,conditions,"leftouter").select(fields_to_select)
        filter_cols1 = []
        filter_cols2 = []

        for col in primary_cols:
            filter_cols1.append(joinDF[col])
            filter_cols1.append(joinDF['masked_'+col])

            filter_cols2.append(joinDF['Target_'+col])
            filter_cols2.append(joinDF['Target_masked_'+col])

        filterDF = joinDF.filter(xxhash64(*filter_cols1)
                                !=xxhash64(*filter_cols2))

        concat_cols = [filterDF[col] for col in primary_cols]
        mergeDF = filterDF.withColumn("MERGEKEY",concat(*concat_cols))

        condition = "concat("
        condition_lst = []
        for i in primary_cols:
            condition_lst.append('target.'+i)
        condition+=','.join(condition_lst)+")=source.MERGEKEY and target.active_flag='Y'"

        dummyDF = mergeDF.filter("Target_advertising_id is not null").withColumn("MERGEKEY",lit(None))
        scdDF = mergeDF.union(dummyDF)
        values = dict()
        for col in primary_cols:
            values[col] = "source."+col
            values["masked_"+col] = "source.masked_"+col
        
        targetTable = DeltaTable.forPath(sc,"s3://stagingzone5077/Delta/"+self.dataset+"/")
        targetTable.alias("target").merge(
            source = scdDF.alias('source'),
            condition = condition
            ).whenMatchedUpdate(set =
                            {
                                "active_flag":"'N'",
                                "end_date":"current_date"
                            }
                        ).whenNotMatchedInsert(
                        values=values
                        ).execute()



if __name__=='__main__':

    
    dataset_to_be_processed = sys.argv[1]
    spark_config_loc = sys.argv[2]
    dataset_file = sys.argv[3]
    transform_obj = TransformData()
    conf_obj = Configuration(spark_config_loc,dataset_to_be_processed)
    day = date.today()

    # Loading dataset from Raw Zone for processing
    path = "s3://"+conf_obj.raw_bucket+"/"+dataset_file
    df = sc.read.option('header',True).parquet(path)

    #-----------------[Data Transformation & Masking]---------------
    # convertDatatype method is called for the conversion
    df = transform_obj.convertDatatype(conf_obj.df_datatypes,df)

    #The conversion of array to string 
    df = df.withColumn('location_source',concat_ws(',',col('location_source')))

    #Rounding the decimals to 7 points
    if dataset_to_be_processed.lower()=='actives':
        df = transform_obj.convertDecimals(df,'user_latitude',7)
        df = transform_obj.convertDecimals(df,'user_longitude',7)
    else:
        df = transform_obj.convertDecimals(df,'user_lat',7)
        df = transform_obj.convertDecimals(df,'user_long',7)

    #Converting string field to date-time field
    df = df.withColumn('date',to_date(col('date'),'yyyy-mm-dd'))
    
    for column in conf_obj.df_maskColumns:
        df = transform_obj.maskColumns(column,df)

    
    #adding historical data to delta table
    delta_add = Add_to_delta(df,dataset_to_be_processed,spark_config_loc)
    delta_add.process()

    primary_cols = conf_obj.pii_cols
    for i in primary_cols:
        df = df.drop(i)
    #-----------------[Raw_Zone ---> Staging_Zone]--------------
    transform_obj.moveFilesWithPartition(df,'parquet',conf_obj.partition_cols,conf_obj.staging_zone_dest+dataset_to_be_processed+"/")





