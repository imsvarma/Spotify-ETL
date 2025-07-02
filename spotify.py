import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node album
album_node1751420474659 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdaata-imani/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1751420474659")

# Script generated for node artist
artist_node1751420476046 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdaata-imani/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1751420476046")

# Script generated for node tracks
tracks_node1751420476533 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdaata-imani/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1751420476533")

# Script generated for node SQL Query
SqlQuery796 = '''
SELECT
  a.*,
  ar.name AS artist_name,
  ar.genre,
  ar.followers
FROM
  a
INNER JOIN
  ar
ON
  TRIM(LOWER(a.artist_id)) = TRIM(LOWER(ar.id))
'''
SQLQuery_node1751423005324 = sparkSqlQuery(glueContext, query = SqlQuery796, mapping = {"a":album_node1751420474659, "ar":artist_node1751420476046}, transformation_ctx = "SQLQuery_node1751423005324")

# Script generated for node Join
Join_node1751423903351 = Join.apply(frame1=SQLQuery_node1751423005324, frame2=tracks_node1751420476533, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Join_node1751423903351")

# Script generated for node Drop Fields
DropFields_node1751423971298 = DropFields.apply(frame=Join_node1751423903351, paths=["id"], transformation_ctx="DropFields_node1751423971298")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1751423971298, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751424430639", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1751425076442 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1751423971298, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-datewithdaata-imani/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1751425076442")

job.commit()