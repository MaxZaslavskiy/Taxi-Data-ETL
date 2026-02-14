using CsvHelper.Configuration;

namespace TaxiDataImporter
{
    public class TripMap : ClassMap<TripRecord>
    {
        public TripMap()
        {
            Map(m => m.PickupDatetime).Name("tpep_pickup_datetime");
            Map(m => m.DropoffDatetime).Name("tpep_dropoff_datetime");
            Map(m => m.PassengerCount).Name("passenger_count");
            Map(m => m.TripDistance).Name("trip_distance");
            Map(m => m.StoreAndFwdFlag).Name("store_and_fwd_flag");
            Map(m => m.PULocationID).Name("PULocationID");
            Map(m => m.DOLocationID).Name("DOLocationID");
            Map(m => m.FareAmount).Name("fare_amount");
            Map(m => m.TipAmount).Name("tip_amount");
        }
    }
}