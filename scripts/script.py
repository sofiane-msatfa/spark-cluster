import os
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, to_date, lpad

import psycopg2
from psycopg2 import sql
from multiprocessing import Pool, cpu_count

DEBUG = os.environ.get("DEBUG", "False") == "True"


class DBConfig:
    HOST = os.environ.get("DB_HOST", "localhost")
    PORT = os.environ.get("DB_PORT", "5432")
    DATABASE = os.environ.get("DB_NAME", "postgres")
    USER = os.environ.get("DB_USER", "postgres")
    PASSWORD = os.environ.get("DB_PASSWORD", "password")

    def get_conn(self):
        """Create a connection to the PostgreSQL database."""
        return psycopg2.connect(
            host=self.HOST,
            port=self.PORT,
            dbname=self.DATABASE,
            user=self.USER,
            password=self.PASSWORD,
        )


class File:
    def __init__(self, path: str, size: int, owner: str, date: str):
        self.path = path
        self.extension = path.split(".")[-1]
        self.size = size
        self.name = path.split("/")[-1].split(".")[0]
        self.owner = owner
        self.datetime = datetime.strptime(date, "%Y-%m-%d %H:%M")

    def __repr__(self):
        return f"{self.name} ({self.size} bytes) by {self.owner} on {self.datetime}"

    def __str__(self):
        return self.__repr__()


# Créer la session Spark
spark = (
    SparkSession.builder.appName("FlightDataAnalysis")
    .master("spark://hadoop-namenode:7077")
    .getOrCreate()
)


def create_file_from_stdout(line: str) -> File:
    parts = line.split()
    path = parts[-1]
    size = int(parts[4])
    owner = parts[2]
    date = f"{parts[5]} {parts[6]}"
    return File(path, size, owner, date)


def list_hdfs_files(filepath: str, extensions: list[str] = ["csv"]) -> list[File]:
    try:
        lines = (
            subprocess.check_output(["hdfs", "dfs", "-ls", filepath], text=True)
            .strip()
            .split("\n")
        )
        files = [
            create_file_from_stdout(line) for line in lines if line.startswith("-")
        ]
        return [file for file in files if file.extension in extensions]
    except subprocess.CalledProcessError as e:
        print(f"Error listing files in {filepath}: {e}\n") if DEBUG else None
        return []


def copy_to_postgres(table_name: str, file: File) -> None:
    conn = DBConfig().get_conn()
    cursor = conn.cursor()

    try:
        hdfs_file_content = subprocess.check_output(
            ["hdfs", "dfs", "-cat", file.path], text=True
        )
        cursor.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(table_name)
            ),
            hdfs_file_content,
        )
        conn.commit()  # Valider les changements
    except Exception as e:
        print(f"Error copying {file} to {table_name}: {e}\n") if DEBUG else None
    finally:
        cursor.close()
        conn.close()


def create_temp_chunks_dir():
    output_dir = f"/tmp/spark_output/{datetime.now().strftime('%Y%m%d%H%M%S')}"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", output_dir], check=True)
    print(f"Created HDFS directory: {output_dir}\n") if DEBUG else None
    return output_dir


def create_temp_chunks_files(df, output_dir: str, partition_factor=2) -> list[File]:
    num_partitions = cpu_count() * partition_factor  # Dépend du nombre de CPU
    (
        print(
            f"Writing DataFrame in {num_partitions} partitions to HDFS at {output_dir}"
        )
        if DEBUG
        else None
    )
    df.repartition(num_partitions).write.option("maxRecordsPerFile", 100000).mode(
        "overwrite"
    ).csv(output_dir)

    return list_hdfs_files(output_dir)


def clean_temp_chunks_files(output_dir: str) -> None:
    try:
        subprocess.run(["hdfs", "dfs", "-rm", "-r", output_dir], check=True)
        print(f"Cleaned HDFS directory: {output_dir}") if DEBUG else None
    except subprocess.CalledProcessError as e:
        print(f"Error cleaning HDFS directory {output_dir}: {e}") if DEBUG else None


create_on_time_performance_table = """
    CREATE TABLE IF NOT EXISTS on_time_performance (
        FlightDate DATE,
        Year NUMERIC(4),
        Month NUMERIC(2),
        DayofMonth NUMERIC(2),
        DayOfWeek NUMERIC(1),
        OriginAirportID INT,
        DestAirportID INT,
        Operating_Airline INT,
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
        LateAircraftDelay INT
    );
    """


def ensure_on_time_performance_table_exists():
    conn = DBConfig().get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(create_on_time_performance_table)
        conn.commit()
        print("Table on_time_performance created successfully") if DEBUG else None
    except Exception as e:
        print(f"Error creating table on_time_performance: {e}") if DEBUG else None
    finally:
        cursor.close()
        conn.close()


def process_on_time_performance_csv(file: File):
    print(f"Processing {file.path}...\n") if DEBUG else None
    try:
        df = spark.read.csv(file.path, header=True, inferSchema=True)
        df = df.dropDuplicates()  # Supprimer les doublons
        df = transform_data(df)
        tmp_dir = create_temp_chunks_dir()
        chunks = create_temp_chunks_files(df, tmp_dir)
        with Pool(cpu_count()) as pool:
            pool.starmap(
                copy_to_postgres,
                [("on_time_performance", chunk) for chunk in chunks],
            )

        clean_temp_chunks_files(tmp_dir)
        print(f"{len(chunks)} chunks processed from {file.path}") if DEBUG else None
    except Exception as e:
        print(f"Error processing {file.path}: {e}\n") if DEBUG else None


def process_data(path: str, fn) -> None:
    files = list_hdfs_files(path)
    print(f"{len(files)} files found in {path}\n") if DEBUG else None

    for file in files:
        fn(file)

def transform_data(df):
    """
    Transforme les données avant l'insertion dans PostgreSQL.
    """
    df = df.select([col(col).alias(col.strip()) for col in df.columns])

    # Garder uniquement les colonnes nécessaires
    df = df.select(
        "FlightDate",
        "Year",
        "Month",
        "DayofMonth",
        "DayOfWeek",
        "OriginAirportID",
        "DestAirportID",
        "Operating_Airline",
        "CRSDepTime",
        "DepTime",
        "CRSArrTime",
        "ArrTime",
        "WheelsOff",
        "WheelsOn",
        "Cancelled",
        "CRSElapsedTime",
        "ActualElapsedTime",
        "AirTime",
        "Flights",
        "Distance",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
    )
    
    # Convertir FlightDate en format DATE (yyyy-mm-dd)
    df = df.withColumn("FlightDate", to_date(col("FlightDate").cast("string"), "yyyyMMdd"))
    
    # Convertir les colonnes numériques entières
    int_columns = ["Year", "Month", "DayofMonth", "DayOfWeek", "OriginAirportID", "DestAirportID", 
                   "Operating_Airline", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights", 
                   "Distance", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
                   "CRSDepTime", "DepTime", "CRSArrTime", "ArrTime", "WheelsOff", "WheelsOn"]
    
    for col_name in int_columns:
        df = df.withColumn(col_name, col(col_name).cast("int"))
    
    # Convertir Cancelled en BOOLEAN
    df = df.withColumn("Cancelled", when(col("Cancelled") == 1, True).otherwise(False))
    
    # Gérer les vols annulés (remplacer les colonnes liées au temps par NULL si annulé)
    df = df.withColumn("DepTime", when(col("Cancelled"), None).otherwise(col("DepTime")))
    df = df.withColumn("ArrTime", when(col("Cancelled"), None).otherwise(col("ArrTime")))
    df = df.withColumn("ActualElapsedTime", when(col("Cancelled"), None).otherwise(col("ActualElapsedTime")))
    df = df.withColumn("AirTime", when(col("Cancelled"), None).otherwise(col("AirTime")))
    df = df.withColumn("CarrierDelay", when(col("Cancelled"), None).otherwise(col("CarrierDelay")))
    df = df.withColumn("WeatherDelay", when(col("Cancelled"), None).otherwise(col("WeatherDelay")))
    df = df.withColumn("NASDelay", when(col("Cancelled"), None).otherwise(col("NASDelay")))
    df = df.withColumn("SecurityDelay", when(col("Cancelled"), None).otherwise(col("SecurityDelay")))
    df = df.withColumn("LateAircraftDelay", when(col("Cancelled"), None).otherwise(col("LateAircraftDelay")))

    return df


def main():
    ensure_on_time_performance_table_exists()
    process_data(
        path="/staging/On_Time_Marketing_Carrier_On_Time_Performance*",
        fn=process_on_time_performance_csv,
    )


if __name__ == "__main__":
    main()
