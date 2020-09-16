def query(schema,table):
    
    query = '''
    select * from {schema}.{table}
    limit 1
    '''.format(schema=schema,table=table)
    
    return query