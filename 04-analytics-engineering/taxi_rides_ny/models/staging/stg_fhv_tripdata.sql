SELECT
    -- Identifiers (standardized naming for cosistency)
    CAST(dispatching_base_num AS string) AS dispatching_base_num,
    CAST(PUlocationID AS int) AS pickup_location_id,
    CAST(DOlocationID AS int) AS dropoff_location_id,
    
    -- Timestamps(standardized naming)
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropOff_datetime AS timestamp) AS dropoff_datetime,

    CAST(SR_Flag AS string) AS sr_flag,
    CAST(Affiliated_base_number AS string) AS affiliated_base_number

FROM {{ source('raw_data', 'fhv_tripdata_2019') }}

-- Filter out records with null dispatching_base_num (data quality requirment)
WHERE dispatching_base_num IS NOT NULL
