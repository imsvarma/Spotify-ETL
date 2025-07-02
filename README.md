# Spotify-ETL
Workflow steps:

1. Data Sources (S3 Buckets):
Album: Reads album metadata from the S3 location.
Artist: Reads artist information from the S3 location.
Tracks: Reads track details from the S3 location.

2. SQL Query Transformation:
Performs an SQL join between the Album and Artist datasets.
This join enriches album data with artist details like artist name, genre, and followers.

3. Join Transformation:
Takes the output from the SQL query (joined album and artist data) and joins it with the Tracks dataset.
This adds track-specific information, associating tracks with enriched album and artist data.

4.Drop Fields Transformation:
Removes unnecessary or duplicate columns from the joined dataset to optimize the output schema and reduce data size.

5. Data Target - Amazon S3:
Writes the final transformed dataset back to an S3 bucket in a structured format (likely Parquet or CSV).
This output is ready for downstream analytics, querying via Athena, or BI visualization.

Summary
The workflow efficiently merges data from three different sources to create a comprehensive dataset combining albums, artists, and tracks.
The final output stored in S3 serves as a curated dataset optimized for querying and analysis.
This ETL process can be scheduled or triggered to keep your data warehouse updated.

