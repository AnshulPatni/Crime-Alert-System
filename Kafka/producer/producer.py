

"""
{
  "instance_id": "f2072a45-bda0-431a-8346-9fccc3b13057",
  "mqlight_lookup_url": "https://mqlight-lookup-prod02.messagehub.services.us-south.bluemix.net/Lookup?serviceId=f2072a45-bda0-431a-8346-9fccc3b13057",
  "api_key": "aBK1K5xyorerlBe0HSNyrLogsaif0pd803ayUJLBqZV9EuNp",
  "kafka_admin_url": "https://kafka-admin-prod02.messagehub.services.us-south.bluemix.net:443",
  "kafka_rest_url": "https://kafka-rest-prod02.messagehub.services.us-south.bluemix.net:443",
  "kafka_brokers_sasl": [
    "kafka04-prod02.messagehub.services.us-south.bluemix.net:9093",
    "kafka01-prod02.messagehub.services.us-south.bluemix.net:9093",
    "kafka05-prod02.messagehub.services.us-south.bluemix.net:9093",
    "kafka02-prod02.messagehub.services.us-south.bluemix.net:9093",
    "kafka03-prod02.messagehub.services.us-south.bluemix.net:9093"
  ],
  "user": "xxxxx",
  "password": "xxxxxx"
}

"""

bs_server = "kafka04-prod02.messagehub.services.us-south.bluemix.net:9093," \
            "kafka03-prod02.messagehub.services.us-south.bluemix.net:9093," \
            "kafka02-prod02.messagehub.services.us-south.bluemix.net:9093," \
            "kafka01-prod02.messagehub.services.us-south.bluemix.net:9093," \
            "kafka05-prod02.messagehub.services.us-south.bluemix.net:9093"




try:
    from confluent_kafka import Producer
except:
    print("NA")


def on_delivery(err2, msg):
    print("In delivery")
    if err2:
        print('Delivery report: Failed sending message {0}'.format(msg.value()))
        print(err2)
        # We could retry sending the message
    else:
        print('Message produced, offset: {0}'.format(msg.offset()))
        print(msg.partition())
        print(msg.key())


driver_options = {
            'bootstrap.servers': bs_server,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': "xxxxxx",
            'sasl.password': "xxxxxxx",
            'api.version.request': True,
            'client.id': 'kafka-python-console-sample-producer',

        }


prd = Producer(driver_options)

i = 0
filename = '/Users/nikhila/Downloads/crime_data.csv'





import csv
str = ""
try:
    with open(filename, 'r') as csvf :
        sr = csv.reader(csvf, delimiter=',', quotechar='"')
        for r in sr:
            #print(type(r))
            str = '|'.join(r)
            print(str)
            prd.produce("test2", str, "crime_data", on_delivery=on_delivery)
        prd.poll(0)
except Exception as e :
    print(e)



prd.flush()
