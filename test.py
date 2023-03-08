from influxdb import InfluxDBClient

my_Client = InfluxDBClient(
    host = 'somedomain.com',
    port = 8086,
    username = 'anonymous',
    password = "somepass",
    ssl = True,
    verify_ssl = True
    )

client.get_list_database()
