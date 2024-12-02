from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_YaDirect_Custom_1").getOrCreate()

YaDirectCustom1 = StructType([
    StructField('Date', DateType(), True),
    StructField('CampaignId', StringType(), True),
    StructField('CampaignName', StringType(), True),
    StructField('CampaignType', StringType(), True),
    StructField('AdGroupId', StringType(), True),
    StructField('AdGroupName', StringType(), True),
    StructField('AdId', StringType(), True),
    StructField('AdFormat', StringType(), True),
    StructField('AdNetworkType', StringType(), True),
    StructField('CarrierType', StringType(), True),
    StructField('Device', StringType(), True),
    StructField('MobilePlatform', StringType(), True),
    StructField('Gender', StringType(), True),
    StructField('Age', StringType(), True),
    StructField('IncomeGrade', StringType(), True),
    StructField('TargetingLocationName', StringType(), True),
    StructField('LocationOfPresenceName', StringType(), True),
    StructField('Criterion', StringType(), True),
    StructField('CriterionType', StringType(), True),
    StructField('Placement', StringType(), True),
    StructField('TargetingCategory', StringType(), True),
    StructField('Slot', StringType(), True),
    StructField('Impressions', StringType(), True),
    StructField('Clicks', StringType(), True),
    StructField('Cost', StringType(), True),
    StructField('Sessions', StringType(), True),
    StructField('Bounces', StringType(), True),
    StructField('AvgClickPosition', StringType(), True),
    StructField('AvgEffectiveBid', StringType(), True),
    StructField('AvgImpressionPosition', StringType(), True),
    StructField('AvgPageviews', StringType(), True),
    StructField('AvgTrafficVolume', StringType(), True),
    StructField('Revenue', StringType(), True),
    StructField('Conversions', StringType(), True)
])

df1 = spark.read \
	.option("delimiter", "\t") \
	.schema(YaDirectCustom1) \
	.csv("/data/yaDirect/txt_custom_report_1/YaDirect_custum1.txt")

df2 = df1.select(
    col('Date'),
    col('CampaignId').cast(LongType()),
    col('CampaignName'),
    col('CampaignType'),
    col('AdGroupId').cast(LongType()),
    col('AdGroupName'),
    col('AdId').cast(LongType()),
    col('AdFormat'),
    col('AdNetworkType'),
    col('CarrierType'),
    col('Device'),
    col('MobilePlatform'),
    col('Gender'),
    col('Age'),
    col('IncomeGrade'),
    col('TargetingLocationName'),
    col('LocationOfPresenceName'),
    col('Criterion'),
    col('CriterionType'),
    col('Placement'),
    col('TargetingCategory'),
    col('Slot'),
    col('Impressions').cast(LongType()),
    col('Clicks').cast(LongType()),
    col('Cost').cast(LongType()),
    col('Sessions').cast(LongType()),
    col('Bounces').cast(LongType()),
    col('AvgClickPosition').cast(DecimalType()),
    col('AvgEffectiveBid').cast(LongType()),
    col('AvgImpressionPosition').cast(DecimalType()),
    col('AvgPageviews').cast(DecimalType()),
    col('AvgTrafficVolume').cast(DecimalType()),
    col('Revenue').cast(LongType()),
    col('Conversions').cast(LongType())
).filter("Date is not null").distinct()

df2.show(3)

df2.write.format("orc") \
			.mode("append") \
			.partitionBy("Date") \
			.option("maxRecordsPerFile", 150000) \
			.save("/data/yaDirect/custom_report_1")