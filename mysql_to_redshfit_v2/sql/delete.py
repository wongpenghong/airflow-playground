def query(date,schema,table,column):
    query_delete = '''
        DELETE FROM {schema}.{table} WHERE id = 
        (select id FROM {schema}.{table} WHERE date({columns}) = '{date}')
        and date({columns}) != '{date}'; '''.format(date=date,schema=schema,table=table,columns=column)
    
    return query_delete