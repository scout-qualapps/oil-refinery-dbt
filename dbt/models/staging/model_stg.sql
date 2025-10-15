{{ config(
    alias='oil_refinery_base',
    materialized='incremental'
) }}

WITH parsed AS (
    SELECT
        _AIRBYTE_RAW_ID,
        ID,
        TYPE,
        GEOMETRY   AS geometry_json,
        PROPERTIES AS properties_json
    FROM {{ source('oil_refinery_esri', 'oil_refinery_raw') }}
),
extracted AS (
    SELECT
        _AIRBYTE_RAW_ID,
        ID,
        TYPE,
        geometry_json:type::string AS geometry_type,
        geometry_json:coordinates[0]::float AS longitude,
        geometry_json:coordinates[1]::float AS latitude
    
    FROM parsed
)
SELECT * FROM extracted
