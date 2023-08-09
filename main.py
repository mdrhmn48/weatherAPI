from datetime import date

from pyncei import NCEIBot, NCEIResponse


# Initialize NCEIBot object using your token string
ncei = NCEIBot("neRbOTsqdZCEWEdDgcZeFMeVPUyfZcgW", cache_name="ncei")

# Set the date range
mindate = date(2016, 1, 1)  # either yyyy-mm-dd or a datetime object
maxdate = date(2019, 12, 31)

# Get all DC stations operating between mindate and maxdate
stations = ncei.get_stations(
    datasetid="GHCND",
    datatypeid=["TMIN", "TMAX"],
    locationid="FIPS:11",
    startdate=mindate,
    enddate=maxdate,
)

# Select the station with the best data coverage
station = sorted(stations.values(), key=lambda s: -int(s["datacoverage"]))[0]

# Get temperature data for the given dates. Note that for the
# data endpoint, you can't request more than one year's worth of daily
# data at a time.
year = maxdate.year
response = NCEIResponse()
while year >= mindate.year:
    response.extend(
        ncei.get_data(
            datasetid="GHCND",
            # stationid=station["USC00390043"],
            datatypeid=["TMIN", "TMAX"],
            startdate=date(year, 1, 1),
            enddate=date(year, 12, 31),
        )
    )
    year -= 1

# Save values to CSV using the to_csv method
response.to_csv(station["id"].replace(":", "") + ".csv")

# Alternatively, merge observation and station data together in a pandas
# DataFrame. If geopandas is installed and coordinates are given, this
# method will return a GeoDataFrame instead.
df_stations = stations.to_dataframe()
df_response = response.to_dataframe()
df_merged = df_stations.merge(df_response, left_on="id", right_on="station")

# Transform data if needed (convert to JSON, CSV, etc.)

# Upload data to S3
# s3 = boto3.client('s3')
# bucket_name = 'your-s3-bucket-name'
# file_name = 'weather_data.json'  # Adjust the file name

# s3.upload_file(file_name, bucket_name, file_name)
# print(f"{file_name} uploaded to {bucket_name}")
