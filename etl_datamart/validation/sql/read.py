def query(table,schema,condition,path):
    queryFile = open('{path}/sql/sql_file/{table}.sql'.format(table=table,path=path), 'r')
    queryFile = queryFile.read()
    
    if condition == 0:    
        query = '''
        SELECT 
        '{1}' as "schemas",
        '{2}' as tables,
        COUNT(1) as total 
        FROM
        ({0})
        '''.format(queryFile,schema,table)
        
    elif condition == 1: 
        query = '''
        SELECT 
        '{schema}' as "schemas",
        '{table}' as tables,
        COUNT(1) as total 
        FROM {schema}.{table}
        '''.format(schema=schema,table=table)
    
    else:
        print('NEED TO CHECK CONDITION EITHER 0 OR 1')
    
    print(query)
        
    return query