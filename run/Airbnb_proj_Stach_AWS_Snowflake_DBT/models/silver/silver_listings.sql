-- back compat for old kwarg name
  
  begin;
    

        insert into AIRBNB.silver.silver_listings ("LISTING_ID", "HOST_ID", "PROPERTY_TYPE", "ROOM_TYPE", "CITY", "COUNTRY", "ACCOMMODATES", "BEDROOMS", "BATHROOMS", "BEDROOM_DENSITY", "PRICE_PER_NIGHT", "PRICE_PER_PERSON", "PRICE_PER_NIGHT_TAG", "LISTING_CREATED_AT", "ETL_LOADED_AT")
        (
            select "LISTING_ID", "HOST_ID", "PROPERTY_TYPE", "ROOM_TYPE", "CITY", "COUNTRY", "ACCOMMODATES", "BEDROOMS", "BATHROOMS", "BEDROOM_DENSITY", "PRICE_PER_NIGHT", "PRICE_PER_PERSON", "PRICE_PER_NIGHT_TAG", "LISTING_CREATED_AT", "ETL_LOADED_AT"
            from AIRBNB.silver.silver_listings__dbt_tmp
        );
    commit;