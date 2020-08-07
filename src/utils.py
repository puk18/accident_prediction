from os.path import isdir
from pyspark.sql import SparkSession
from pyspark import SparkConf
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from workdir import workdir


def raise_parquet_not_del_error(cache):
    ''' Raise an error if cache parquet has not been deleted.
    '''
    if isdir(cache):
        print('Failed to remove parquet directory/file')
        raise Exception('Failed to remove parquet directory/file')
    return


def init_spark():
     
     sess = (SparkSession
            .builder
            .appName("Accident prediction")
            .config("spark.rdd.compress", "True")
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer")
            .config("spark.cleaner.periodicGC.interval", "5min")
            # .config("spark.executor.memory","2g") 
            .config("spark.network.timeout", "300s")
            .config("spark.driver.memory", "4g")
            .getOrCreate())
#   
   
        
     print('Spark Session created')
     print('Parameters:')
     for param in sess.sparkContext.getConf().getAll():
         print(f"\t{param[0]}: {param[1]}")
     return sess




def get_with_retry(
    url,
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session.get(url)
