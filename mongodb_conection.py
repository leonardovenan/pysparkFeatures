df = spark.read\
.format("mongo")\
.option("uri", "mongodb+srv://user:password@cluster/database")\
.option("database", "database")\
.option("collection", "collection")\
.load() 