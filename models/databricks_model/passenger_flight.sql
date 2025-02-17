with passenger_details as (
    SELECT p.*
FROM {{ ref('filght_passenger') }} p
JOIN {{ ref('passenger_airline') }} a
ON p.FlightNumber = a.FlightNumber
)
SELECT * from passenger_details