"""
TODO: Télécharger les données de référence pour les aéroports, les compagnies aériennes, les codes de pays et les codes de région.
Et créer les scripts pour les insérer dans la base de données.
"""

# https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=GEI&QO_fu146_anzr=N8vn6v10%20f722146%20gnoyr5
create_world_area_codes_table = (
    """
    CREATE TABLE IF NOT EXISTS world_area_codes (
        Wac INT PRIMARY KEY,
        WacSeqID2 INT,
        WacName TEXT,
        CountryName TEXT,
        CountryCode CHAR(2),
        StateCode TEXT,
        StateName TEXT,
        IsLatest BOOLEAN
    );
    """
)
# https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FLL&QO_fu146_anzr=N8vn6v10%20f722146%20gnoyr5
create_master_coordinates_table = (
    """
    CREATE TABLE IF NOT EXISTS master_coordinates (
        AirportID INT PRIMARY KEY,
        AirportSeqID INT,
        Airport TEXT,
        AirportName TEXT,
        AirportCityName TEXT,
        AirportCountryName TEXT,
        AirportCountryCodeISO CHAR(2),
        AirportStateName TEXT,
        AirportStateCode CHAR(2),
        AirportStateName TEXT,
        AirportWac INT REFERENCES world_area_codes(Wac),
        AirportIsLatest BOOLEAN
    );
    """
)
# https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=GDH&QO_fu146_anzr=N8vn6v10%20f722146%20gnoyr5
create_carrier_decode_table = (
    """
    CREATE TABLE IF NOT EXISTS carrier_decode (
        UniqueCarrier TEXT PRIMARY KEY,
        UniqCarrierEntity TEXT,
        UniqueCarrierName TEXT,
        AirlineID INT, -- not unique
        Carrier TEXT, -- Carrier field in df
        CarrierEntity TEXT,
        CarrierName TEXT,
        WAC INT REFERENCES world_area_codes(Wac)
    );
    """
)
# https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr
create_on_time_performance_table = (
    """
    CREATE TABLE IF NOT EXISTS on_time_performance (
        FlightDate DATE,
        Year INT,
        Month INT,
        DayofMonth INT,
        DayOfWeek INT,
        OriginAirportID INT REFERENCES master_coordinates(AirportID),
        DestAirportID INT REFERENCES master_coordinates(AirportID),
        Operating_Airline TEXT REFERENCES carrier_decode(UniqueCarrier),
        CRSDepTime INT,
        DepTime INT,
        CRSArrTime INT,
        ArrTime INT,
        WheelsOff INT,
        WheelsOn INT,
        Cancelled BOOLEAN,
        CRSElapsedTime INT,
        ActualElapsedTime INT,
        AirTime INT,
        Flights INT,
        Distance INT,
        CarrierDelay INT,
        WeatherDelay INT,
        NASDelay INT,
        SecurityDelay INT,
        LateAircraftDelay INT,
    );
    """
)
# https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FJD&QO_fu146_anzr=Nv4%20Pn44vr45
create_t_100_carrier_table = (
    """
    CREATE TABLE IF NOT EXISTS t_100_carrier (
        Year NUMERIC(4),
        Month NUMERIC(2),
        OriginAirportID INT REFERENCES master_coordinates(AirportID),
        DestAirportID INT REFERENCES master_coordinates(AirportID),
        UniqueCarrier INT REFERENCES carrier_decode(UniqueCarrier),
        Passengers INT,
        Freight INT,
        Mail INT,
        Distance INT
    );
    """
)

on_time_performance_insert = (
    """
    INSERT INTO on_time_performance (
        FlightDate, Year, Month, DayofMonth, DayOfWeek, OriginAirportID, DestAirportID, Operating_Airline,
        CRSDepTime, DepTime, CRSArrTime, ArrTime, WheelsOff, WheelsOn, Cancelled, CRSElapsedTime,
        ActualElapsedTime, AirTime, Flights, Distance, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
        LateAircraftDelay
    ) VALUES (
        %(FlightDate)s, %(Year)s, %(Month)s, %(DayofMonth)s, %(DayOfWeek)s, %(OriginAirportID)s, %(DestAirportID)s,
        %(Operating_Airline)s, %(CRSDepTime)s, %(DepTime)s, %(CRSArrTime)s, %(ArrTime)s, %(WheelsOff)s, %(WheelsOn)s,
        %(Cancelled)s, %(CRSElapsedTime)s, %(ActualElapsedTime)s, %(AirTime)s, %(Flights)s, %(Distance)s,
        %(CarrierDelay)s, %(WeatherDelay)s, %(NASDelay)s, %(SecurityDelay)s, %(LateAircraftDelay)s
    );
    """
)

t_100_carrier_insert = (
    """
    INSERT INTO t_100_carrier (
        Year, Month, OriginAirportID, DestAirportID, UniqueCarrier, Passengers, Freight, Mail, Distance
    ) VALUES (
        %(Year)s, %(Month)s, %(OriginAirportID)s, %(DestAirportID)s, %(UniqueCarrier)s, %(Passengers)s, %(Freight)s,
        %(Mail)s, %(Distance)s
    );
    """
)