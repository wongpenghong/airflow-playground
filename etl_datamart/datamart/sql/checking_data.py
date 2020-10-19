def query(path,table):
    queryFile = open('{path}/sql/sql_file/{table}.sql'.format(path=path,table=table), 'r')
    queryFile = queryFile.read()
    
    query = '''SELECT COUNT(1) AS "total" 
    FROM (
    {})
    '''.format(queryFile)
    
    
    return query