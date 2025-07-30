WITH passenger_airline_vw AS  (
SELECT a.*
FROM main.easyjet.passenger_loyalty_data p
JOIN main.easyjet.airline_passenger_data a
ON p.PassengerID = a.PassengerID
)
SELECT * FROM passenger_airline_vw