def query(schema,table,column,date):

    query = '''
    select 
        *
    from 
        {schema}.{table}
    where date({column}) = '{date}'
    '''.format(schema=schema,table=table,column=column,date=date)
    
    return query