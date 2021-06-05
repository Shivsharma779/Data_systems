from pyspark import SparkContext, SparkConf, sql
import sys
import warnings
warnings.filterwarnings('ignore')

try: 
	if (len(sys.argv) > 3 or len(sys.argv)<3):
		print("Wrong output format")
		exit(0)

	if (int(sys.argv[1])< 1):
		print("Min CPU required is 1")
		exit(0)
	spark = sql.SparkSession.builder.appName("PYspark-assignment5")
	spark = spark.master("local")
	spark = spark.config("config", "random")
	spark = spark.getOrCreate()

	df = spark.read.csv('./airports.csv', inferSchema=True, header=True)

	
	
	paritioned_df = df.repartition(int(sys.argv[1]))

	result_rdd = paritioned_df.filter((paritioned_df['LATITUDE']>=10) & (paritioned_df['LATITUDE']<=90)).collect()
	result_df = spark.createDataFrame(result_rdd)


	result_rdd = result_df.filter( (result_df['LONGITUDE']<=-10)  & (result_df['LONGITUDE']>=-90) ).collect()
	result_df = spark.createDataFrame(result_rdd)
	result_df.toPandas()["NAME"].to_csv(sys.argv[2],header=True, index=False, encoding='utf-8')
except Exception as E:
	print("Error", E)