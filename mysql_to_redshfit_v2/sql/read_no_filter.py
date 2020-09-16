def query(schema,table):
    
    query = '''
    select * from {schema}.{table}
    '''.format(schema=schema,table=table)
    
    return query