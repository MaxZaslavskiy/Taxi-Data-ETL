USE TaxiDb;
GO

ALTER TABLE Trips ADD TripDurationMinutes AS DATEDIFF(minute, tpep_pickup_datetime, tpep_dropoff_datetime);
CREATE INDEX IX_Trips_Duration ON Trips (TripDurationMinutes DESC);

CREATE INDEX IX_Trips_Distance ON Trips (trip_distance DESC);

CREATE INDEX IX_Trips_PULocation_Tips ON Trips (PULocationID) INCLUDE (tip_amount);
