SELECT
    acn_id,
    session_id,
    user_id,
    station_id,
    space_id,
    site_id,
    cluster_id,

    -- convert timestamps
    connection_time_raw::timestamp AS connection_time,
    disconnect_time_raw::timestamp AS disconnect_time,
    done_charging_time_raw::timestamp AS done_charging_time,

    timezone,
    kwh_delivered,

    user_inputs_json,
    json_payload,
    ingested_at

FROM {{ source('raw', 'charging_sessions') }}