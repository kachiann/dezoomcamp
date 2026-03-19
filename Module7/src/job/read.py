from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Set up Flink streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

# Kafka table source
t_env.execute_sql("""
CREATE TABLE test_source (
    lpep_pickup_datetime STRING,
    lpep_dropoff_datetime STRING,
    PULocationID INT,
    DOLocationID INT,
    passenger_count INT,
    trip_distance DOUBLE,
    total_amount DOUBLE,
    tip_amount DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'green-trips',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'test-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# Read a few rows and print
t_env.execute_sql("SELECT * FROM test_source LIMIT 5").print()