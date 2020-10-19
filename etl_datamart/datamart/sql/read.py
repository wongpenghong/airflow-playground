def query(table,path,column=0,condition=0,firstDate=0,lastDate=0):
    queryFile = open('{path}/sql/sql_file/{table}.sql'.format(path=path,table=table), 'r')
    queryFile = queryFile.read()
    
    query_first = '''
    {}
    limit 1
    '''.format(queryFile)
    
    if condition == 0:
        query = '''
        {}
        '''.format(queryFile)
        
    elif condition == 1:   
        query = '''SELECT * FROM
        (
        {query})
        WHERE DATE({column}) BETWEEN '{firstDate}' and '{lastDate}'
        '''.format(query=queryFile,column=column,firstDate=firstDate,lastDate=lastDate)
    
    return query_first,query