import pytest
from pyspark.sql import functions as F

df_unity_test = spark.sql("""
    with repetion as (
        select ff___contrato, count(0) from dbstage.tb_relatorio_comissao
        group by ff___contrato
        having count(0) > 1
    ), sum_groupby as (
      select
        a.ff___contrato,
        sum(cast(REPLACE(REPLACE(valor__base, '.', ''), ',', '.') as float)) as valor__base
      from dbstage.tb_relatorio_comissao a
      inner join repetion b
      on a.ff___contrato = b.ff___contrato
      group by a.ff___contrato
    )
    select * from sum_groupby
    """)

def verificar_soma_valores_zero(df):
    valores_lista = [row['valor__base'] for row in df.collect()]
    return all(valor == 0 for valor in valores_lista), valores_lista

def test_valores_zero(df_spark):
    total_duplicados = df_spark.count()
    if total_duplicados == 0:
        print("✅ Nenhum contrato duplicado encontrado. Teste ignorado.")
        return
    
    todos_zero, valores_lista = verificar_soma_valores_zero(df_spark)
    
    if todos_zero:
        print("✅ Teste passou com sucesso! Todas as somas de valor__base são 0.")
    else:
        mensagem_erro = f"❌ Valores diferentes de 0 encontrados para soma de valores duplicados: {valores_lista}"
        discord_alarm_comissao(mensagem_erro)
        pytest.fail(mensagem_erro) 

    df_spark.show(truncate=False)

test_valores_zero(df_unity_test)
