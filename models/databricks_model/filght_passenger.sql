{{ config(tags=["staging", "etl"]) }}

WITH filght_passenger_vw AS  (
SELECT a.*, f.*
FROM main.easyjet.airline_passenger_data a
JOIN main.easyjet.flight_data f
ON a.FlightNumber = f.FlightNumber
)
SELECT * FROM filght_passenger_vw