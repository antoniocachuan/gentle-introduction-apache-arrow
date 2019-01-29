#Pandas DF to Pyarrow Table
import numpy as np
import pandas as pd
import pyarrow as pa
df = pd.DataFrame({'one': [20, np.nan, 2.5],'two': ['january', 'february', 'march'],'three': [True, False, True]},index=list('abc'))
table = pa.Table.from_pandas(df)

#Read parquet
table2 = pq.read_table('example.parquet')
table2

#Read many parquets in a directory
dataset = pq.ParquetDataset('dataset_name_directory/')
table = dataset.read()
table

#Parquet to Pandas
pdf = pq.read_pandas('example.parquet', columns=['two']).to_pandas()
pdf

#Check metadata
parquet_file = pq.ParquetFile('example.parquet')
parquet_file.metadata

#Timestamp
pq.write_table(table, where, coerce_timestamps='ms')
pq.write_table(table, where, coerce_timestamps='ms', allow_truncated_timestamps=True)

#Compression
pq.write_table(table, where, compression='snappy')
pq.write_table(table, where, compression='gzip')
pq.write_table(table, where, compression='brotli')
pq.write_table(table, where, compression='none')

#Write partitioned parquet file
df = pd.DataFrame({'one': [1, 2.5, 3],
                   'two': ['Peru', 'Brasil', 'Canada'],
                   'three': [True, False, True]},
                   index=list('abc'))
table = pa.Table.from_pandas(df)
pq.write_to_dataset(table, root_path='dataset_name',partition_cols=['one', 'two'])

#Connect HDFS
import pyarrow as pa
host = '1970.x.x.x'
port = 8022
fs = pa.hdfs.connect(host, port)

#Write Parquet to HDFS
pq.write_to_dataset(table, root_path='dataset_name', partition_cols=['one', 'two'], filesystem=fs)

#Read CSV
import pandas as pd
from pyarrow import csv
import pyarrow as pa
fs = pa.hdfs.connect()
with fs.open('iris.csv', 'rb') as f:
 df = pd.read_csv(f, nrows = 10)
df.head()

#Read Parquet I
import pandas as pd
pdIris = pd.read_parquet('hdfs:///iris/part-00000–27c8e2d3-fcc9–47ff-8fd1–6ef0b079f30e-c000.snappy.parquet', engine='pyarrow')
pdTrain.head()

#Read Parquet II
import pyarrow.parquet as pq
path = 'hdfs:///iris/part-00000–71c8h2d3-fcc9–47ff-8fd1–6ef0b079f30e-c000.snappy.parquet'
table = pq.read_table(path)
table.schema
df = table.to_pandas()
df.head()

#Read SAS
import pandas as pd
import pyarrow as pa
fs = pa.hdfs.connect()
with fs.open('/datalake/airplane.sas7bdat', 'rb') as f:
 sas_df = pd.read_sas(f, format='sas7bdat')
sas_df.head()

#Read Excel
import pandas as pd
import pyarrow as pa
fs = pa.hdfs.connect()
with fs.open('/datalake/airplane.xlsx', 'rb') as f:
 g.download('airplane.xlsx')
ex_df = pd.read_excel('airplane.xlsx')

#Read JSON
import pandas as pd
import pyarrow as pa
fs = pa.hdfs.connect()
with fs.open('/datalake/airplane.json', 'rb') as f:
 g.download('airplane.json')
js_df = pd.read_json('airplane.json')

#Download files from HDFS
import pandas as pd
import pyarrow as pa
fs = pa.hdfs.connect()
with fs.open('/datalake/airplane.cs', 'rb') as f:
 g.download('airplane.cs')


#Upload files to HDFS
import pyarrow as pa
fs = pa.hdfs.connect()
with open('settings.xml') as f:
 pa.hdfs.HadoopFileSystem.upload(fs, '/datalake/settings.xml', f)


 #Spark
 from pyspark.sql import SparkSession
warehouseLocation = "/antonio"
spark = SparkSession\
.builder.appName("demoMedium")\
.config("spark.sql.warehouse.dir", warehouseLocation)\
.enableHiveSupport()\
.getOrCreate()

#Create test Spark DataFrame
from pyspark.sql.functions import rand
df = spark.range(1 << 22).toDF("id").withColumn("x", rand())
df.printSchema()

#Create a Pandas DataFrame from a Spark DataFrame using Arrow
%time pdf = df.toPandas()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
%time pdf = df.toPandas()
pdf.describe()

# Create a Spark DataFrame from a Pandas DataFrame using Arrow
%time df = spark.createDataFrame(pdf)
spark.conf.set("spark.sql.execution.arrow.enabled", "false")
%time df = spark.createDataFrame(pdf)
df.describe().show()  
