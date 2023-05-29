import pandas as pd

#criando primeiro dataFrame

df1 = pd.DataFrame({
    'key': ['X', 'Y', 'Z'],
    'valeu': [1, 2, 3]
})

#criando segundo dataFrame

df2 = pd.DataFrame({
    'key': ['A', 'X', 'C', 'Y', 'Z'],
    'value': [4, 5, 6, 7, 8]
})

print(df1)
print(df2)

merge_df = pd.merge(df1, df2, on='key')

print(merge_df)
