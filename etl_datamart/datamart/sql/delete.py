def query(schema,table,column=0,condition=0,start_date=0,end_date=0):
    
    if condition == 0:    
        query = """
            DELETE FROM {schema}.{table}
            """.format(schema=schema,table=table)
    
    elif condition == 1:
        query = """
            DELETE FROM {schema}.{table}
            WHERE DATE({column}) between '{start_date}' and '{end_date}' """.format(
                schema=schema,table=table,column=column,start_date=start_date,end_date=end_date)
            
    return query