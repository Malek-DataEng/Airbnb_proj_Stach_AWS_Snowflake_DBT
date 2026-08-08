-- back compat for old kwarg name
  
  begin;
    

        insert into AIRBNB.silver.silver_hosts ("HOST_ID", "HOST_NAME", "HOST_SINCE", "HOST_TENURE_YEARS", "IS_SUPERHOST", "SUPERHOST_FLAG", "RESPONSE_RATE", "HOST_RESPONSE_SEGMENT", "HOST_CREATED_AT", "ETL_LOADED_AT")
        (
            select "HOST_ID", "HOST_NAME", "HOST_SINCE", "HOST_TENURE_YEARS", "IS_SUPERHOST", "SUPERHOST_FLAG", "RESPONSE_RATE", "HOST_RESPONSE_SEGMENT", "HOST_CREATED_AT", "ETL_LOADED_AT"
            from AIRBNB.silver.silver_hosts__dbt_tmp
        );
    commit;