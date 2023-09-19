import emoji
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

text = "This sample text contains laughing emojis 😀 😃 😄 😁 😆 😅 😂 🤣"

def remove_emojis(text):
    return(emoji.replace_emoji(text, ""))

# for aplication on dataframes
remove_emojis_udf = udf(remove_emojis, StringType())

dfWithoutEmojis = dfdelivery.withColumn("Nome_Completo", remove_emojis_udf("Nome_Completo"))
dfWithoutEmojis.createOrReplaceTempView('teste')
