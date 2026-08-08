
      begin;
    merge into "AIRBNB"."GOLD"."DIM_LISTINGS" as DBT_INTERNAL_DEST
    using "AIRBNB"."GOLD"."DIM_LISTINGS__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     
	
	
	and ((DBT_INTERNAL_DEST.dbt_valid_to = to_date('9999-12-31'))
 or DBT_INTERNAL_DEST.dbt_valid_to is null)

     
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("LISTING_ID", "HOST_ID", "PROPERTY_TYPE", "ROOM_TYPE", "CITY", "COUNTRY", "ACCOMMODATES", "BEDROOMS", "BATHROOMS", "PRICE_PER_NIGHT", "BEDROOM_DENSITY", "PRICE_PER_PERSON", "PRICE_PER_NIGHT_TAG", "LISTING_CREATED_AT", "ETL_LOADED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("LISTING_ID", "HOST_ID", "PROPERTY_TYPE", "ROOM_TYPE", "CITY", "COUNTRY", "ACCOMMODATES", "BEDROOMS", "BATHROOMS", "PRICE_PER_NIGHT", "BEDROOM_DENSITY", "PRICE_PER_PERSON", "PRICE_PER_NIGHT_TAG", "LISTING_CREATED_AT", "ETL_LOADED_AT", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  