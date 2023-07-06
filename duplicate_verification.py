def duplicate_verification(df):
  count_before = df.count()
  count_after = df.dropDuplicates().count()

  if count_before == count_after:
    return("Havent duplicates.")
  else:
    return(f'Duplicates existe. Total: {count_before - count_after}')
    
