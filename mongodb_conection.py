from pymongo import MongoClient
from pyspark.sql import functions as F
from datetime import datetime, timedelta

#general extraction

def conect_mongodb_collection(database, collection_name, schema):
    spark.conf.set("spark.mongodb.input.parseSampleSize", "0")
    df = (spark.read
    .format("mongo")
    .option("uri", f"mongodb+srv://{user}:{password}@{cluster}/{database}?readPreference=secondaryPreferred")
    .option("database", database)
    .option("collection", collection_name)
    .schema(schema)
    .load()
    )
    return df

# yesterday general extraction

def connect_mongodb_collection_yesterday_general(database, collection_name, schema, dateNameField):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    query = {
        f"{dateNameField}": {
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
