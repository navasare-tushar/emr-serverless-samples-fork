
######### Write_s3

def write(spark, df, app_config, refinery_ctrl, r_status):
    write_to = app_config.get_write_to_s3()
    if df.count() > 0:
        # Adding LOAD_TS column with current timestamp
        df = df.withColumn("LOAD_TS", to_timestamp(current_timestamp(), 'yyyy-MM-dd HH24:mm:ss'))
        
        if 'S3' == write_to:
            df, new_status = write_to_s3(spark, df, app_config, refinery_ctrl, r_status)
            ru.msck_repair_athena_tbl(app_config, refinery_ctrl)
            return df, new_status
        
        elif 'Local' == write_to:
            df, new_status = write_to_local(spark, df, app_config, refinery_ctrl, r_status)
            return df, new_status
        
    else:
        r_status.pblshd_status_cd = 'C'
        return df, r_status


def write_to_s3(spark, df, app_config, refinery_ctrl, r_status):
    type_casting_file = app_config.get_publish_zone_select_column_mapping_file()
    with open(type_casting_file) as fl:
        cols = fl.read().splitlines()
        logging.info("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
        logging.info(cols)
        df = df.selectExpr(cols)
    
    cols = ("PROFILE_KEY", "INSTANCE_ID", "CURRENT_ROW_IND", "EXPIRATION_TS", "LOAD_TS", "ERASURE_FLG")
    df = df.drop(*cols)
    
    output_format = app_config.get_output_format()   ### is parquet
    output_location, publish_run_id = ru.get_s3_pbshd_location(app_config, refinery_ctrl)
    
    logging.info("S3 Published zone location: {}".format(output_location))
    if output_format == "parquet":
        df.write.mode("overwrite").parquet(output_location)
    elif output_format == "json":
        df.write.json(output_location)
    elif output_format == "csv":
        df.write.csv(output_location)
    
    r_status.pblshd_run_id = publish_run_id
    r_status.pblshd_status_cd = 'C'
    
    return df, r_status



def get_s3_pbshd_location(app_config, refinery_ctrl):
    publish_run_id = get_runId_loadDate()[0]
    output_location = refinery_ctrl.p2Path + '/' + app_config.get_runId() + '=' + publish_run_id + '/'
    logging.info(output_location)
    return output_location, publish_run_id

def msck_repair_athena_tbl(app_config, refiney_ctrl):
    import boto3
    import json

    # Initialize a boto3 Athena client in the specified region
    client = boto3.client('athena', region_name='us-east-1')

    # Retrieve the Athena database name from the app configuration
    DATABASE = app_config.get_databaseNm_s3()

    # Execute the MSCK REPAIR TABLE command to sync metadata
    client.start_query_execution(
        QueryString='MSCK REPAIR TABLE ' + refiney_ctrl.tableNm + ';',
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': refiney_ctrl.pzPath
        }
    )



#s3://mi-dp-lake-pblshd-useast1-tst/cec/cec_dim_empower_case_detail/batch1234=20241121/
