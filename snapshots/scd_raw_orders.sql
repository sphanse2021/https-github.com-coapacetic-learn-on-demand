{% snapshot scd_raw_orders %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
      target_database='sandbox',
      target_schema=new_schema,
      unique_key='id',
      strategy='timestamp',
      updated_at='_ETL_LOADED_AT',
    )
}}

select * from {{ source('jaffle_shop','orders') }}

{% endsnapshot %}