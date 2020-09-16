def query(schema,table):
    query_delete = '''
        DELETE FROM {schema}.{table}; '''.format(
            schema=schema,table=table)
    
    return query_delete