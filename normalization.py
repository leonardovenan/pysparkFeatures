from pyspark.sql.types import *
from pyspark.sql.functions import *

def func_complex_fields(df):
    complex_fields = dict([(field.name, field.dataType)
                            for field in df.schema.fields
                            if(type(field.dataType) == ArrayType or type(field.dataType) == StructType)
                            ])
    return complex_fields

def normalization(df):
    complex_fields = func_complex_fields(df)
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print(f'Processing: {col_name} Type: {str(type(complex_fields[col_name]))}')
        # Se for StructType então vamos converter todos os sub elementos em colunas        
        if(type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        elif(type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = func_complex_fields(df)
        
    return df
