from snowflake.snowpark.functions import col

def filter_by_territory(session,table_name,territory)
  df = session.table(table_name)
  return df.filter(col("territory") == territory)
def word_count(word_txt):
  pass

