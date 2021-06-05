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

	result_df = paritioned_df.groupby("COUNTRY")
	result_df = result_df.count()


	
	panda_df = result_df.toPandas()
	panda_df.to_csv(sys.argv[2],header=True, index=False)
except Exception as E:
	print("Error", E)