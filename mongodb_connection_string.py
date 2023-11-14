from pyspark.sql import functions as F
from datetime import datetime, timedelta

user = dbutils.secrets.get("SecretsMongo", "USER_MONGO")
password = dbutils.secrets.get("SecretsMongo", "PASSWORD_MONGO")
cluster = dbutils.secrets.get("SecretsMongo", "CLUSTER_URL_MONGO")

def connect_mongodb_collection_yesterday(database, collection_name, schema):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    query = {
        "creationDate": {
            "$gte": yesterday_str + "T00:00:00.000Z",
            "$lt": yesterday_str + "T23:59:59.999Z"
        }
    }

    df = (spark.read
        .format("mongo")
        .option("uri", f"mongodb+srv://{user}:{password}@{cluster}/{database}?readPreference=secondaryPreferred")
        .option("database", database)
        .option("collection", collection_name)
        .option("pipeline", "[{ $match: " + str(query) + "}]")
        .schema(schema)
        .load()
    )
    return df
  
