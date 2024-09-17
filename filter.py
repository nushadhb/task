from snowflake.snowpark.functions import col

def filter_by_role(session, table_name, role):
  df = session.table(table_name)
  return df.filter(col("role") == role)

def filter_by_territory(session,table_name,territory)
  df = session.table(table_name)
<<<<<<< HEAD
  return df.filter(col("territor"y) == territory)
=======
  return df.filter(col("territory") == territory)
>>>>>>> bd5f987e94cbb32db859288766c5883f7f9c7eb9

