def query(schema,table,new_list):
    # get column and type
    ddl = ", ".join(
        [f'"{data["column_name"]}" {data["data_type"]}\n' for data in new_list]
    )
    
    query_create = """
        DROP TABLE IF EXISTS {schema}.{table};
        
        CREATE TABLE IF NOT EXISTS {schema}.{table}
        (
        {ddl});
        
        GRANT ALL PRIVILEGES ON TABLE {schema}.{table} TO airflow;
        """.format(schema=schema,table=table,ddl=ddl)
    
    return query_create