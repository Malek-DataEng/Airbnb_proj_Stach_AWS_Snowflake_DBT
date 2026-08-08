
      begin;
    merge into "AIRBNB"."GOLD"."DIM_HOSTS" as DBT_INTERNAL_DEST
    using "AIRBNB"."GOLD"."DIM_HOSTS__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     
	
	
	and ((DBT_INTERNAL_DEST.dbt_valid_to = to_date('9999-12-31'))
 or DBT_INTERNAL_DEST.dbt_valid_to is null)

     
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("HOST_ID", "HOST_NAME", "HOST_SINCE", "HOST_TENURE_YEARS", "IS_SUPERHOST", "SUPERHOST_FLAG", "RESPONSE_RATE", "HOST_RESPONSE_SEGMENT", "HOST_CREATED_AT", "ETL_LOADED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("HOST_ID", "HOST_NAME", "HOST_SINCE", "HOST_TENURE_YEARS", "IS_SUPERHOST", "SUPERHOST_FLAG", "RESPONSE_RATE", "HOST_RESPONSE_SEGMENT", "HOST_CREATED_AT", "ETL_LOADED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  