{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': ['agent_key'], 'unique': True},
          {'columns': ['agent'], 'unique': False}
        ]
    )
}}

--  Too few data points to make this a full-fledged dimension table.
--  It is also assumed, with the organization being Interpol, that
--  the agent name is unique across the data set. 
--  A badge number would be great to distinguish agents with the same
--  name, having a badge number would also allow creating a slowly
--  changing dimension type 2, maintaining history.
WITH all_data AS
(
    SELECT DISTINCT
        agent
    FROM
        {{ref('combined_sightings')}} cs
)
SELECT
    (ROW_NUMBER() OVER())::BIGINT                                                                                           AS agent_key,
    agent
FROM
    all_data
        
