def query(bucket,key,access_key,secret_key,table,schema):
    query_copy = """
        copy {schema}.{table}
        from 's3://{bucket}/{key}'
            access_key_id '{access_key}'
            secret_access_key '{secret_key}'
            json 'auto';
        """.format(bucket=bucket,key=key,access_key=access_key,secret_key=secret_key,table=table,schema=schema)
    
    return query_copy