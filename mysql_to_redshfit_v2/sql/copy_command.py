def query(bucket,key,access_key,secret_key,table,schema):
    query_copy = """
        copy {schema}.{table}
        from 's3://{bucket}/{key}'
            access_key_id '{access_key}'
            secret_access_key '{secret_key}'
            json 'auto';
        """.format(schema=schema,bucket=bucket,key=key,access_key=access_key,secret_key=secret_key,table=table)
    
    return query_copy