def query(table,new_list,schema):
    # get column and type
    ddl = ", ".join(
        [f'"{data["columns"]}" {data["types"]}\n' for data in new_list]
    )
    
    query_create = """
        DROP TABLE IF EXISTS {schema}."{table}";
        
        CREATE TABLE IF NOT EXISTS {schema}."{table}"
        (
        {ddl});
        
        GRANT ALL PRIVILEGES ON TABLE {schema}.{table} TO airflow;
        
        GRANT ALL PRIVILEGES ON TABLE {schema}.{table} TO metabase;
        """.format(table=table,ddl=ddl,schema=schema)
    
    return query_create