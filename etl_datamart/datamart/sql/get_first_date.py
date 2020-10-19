def query(path,table,column):
    queryFile = open('{path}/sql/sql_file/{table}.sql'.format(path=path,table=table), 'r')
    queryFile = queryFile.read()
    
    query = '''SELECT MIN(DATE({column})) as "date" 
    FROM (
    {query})
    '''.format(query=queryFile,column=column)
    
    
    return query