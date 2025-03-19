%run ../../../utils/NboUtilsGeneral

from pyspark.sql.functions import udf, col, when, expr
from pyspark.sql.types import TimestampType, StringType
import datetime
import json

def convert_to_timestamp(value):
    if value is None:
        return None

    try:
        value_dict = json.loads(value) 
        if "$date" in value_dict:
            return datetime.datetime.fromtimestamp(value_dict["$date"] / 1000.0)  
    except (json.JSONDecodeError, TypeError):
        try:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))  
        except (ValueError, AttributeError):
            return None 

    return None

convert_to_timestamp_udf = udf(convert_to_timestamp, TimestampType())

df_raw = spark.sql("""
                   SELECT * FROM dbraw.tb_growth_restrictions
                   """)

df_norma = normalization(df_raw)
df = (df_norma
        .withColumn(
            "restrictions_created_at",
            when(col("restrictions_created_at").isNotNull(),
                 convert_to_timestamp_udf(col("restrictions_created_at")))
            .otherwise(None)  
        )
        .withColumn(
            "restrictions_updated_at",
            when(col("restrictions_updated_at").isNotNull(),
                 convert_to_timestamp_udf(col("restrictions_updated_at")))
            .otherwise(None) 
        )
    )
df.createOrReplaceTempView('vw_growth')

df_trusted = spark.sql("""
with cte as (
select distinct
    md5(coalesce(_id_oid, '-'))                                 as id_chave
    ,cast(_id_oid                       as string )             as id_creation 
    ,cast(unified_lead_id               as string)              as ch_unified_lead_id
    ,cast(created_at                    as timestamp)           as dt_created_at
    ,cast(updated_at                    as timestamp)           as dt_updated_at
    ,cast(base_unificada_temporaria_id  as string)              as ch_base_unificada_temporaria_id
    ,cast(documents_cpf                 as string)              as ch_documents_cpf
    ,cast(restrictions_name             as string)              as ch_restrictions_name
    ,cast(restrictions_contact          as string)              as ch_restrictions_contact
    ,cast(restrictions_active           as boolean)             as fl_restrictions_active
    ,cast(restrictions_total_restriction as boolean)            as fl_restrictions_total_restriction
    ,cast(restrictions_reason           as string)              as ch_restrictions_reason
    ,cast(restrictions_created_at       as timestamp)           as dt_restrictions_created_at
    ,cast(restrictions_updated_at       as timestamp)           as dt_restrictions_updated_at
    ,cast(dtreferencecreation           as timestamp )          as dt_referencia_criacao
    ,cast(dtreferenceatualization       as timestamp )          as dt_referencia_atualizacao 
    ,row_number() over (partition by _id_oid order by  coalesce(restrictions_updated_at, restrictions_created_at) desc) as rk
from vw_growth)
select * except (rk) from cte
where rk = 1
""")

