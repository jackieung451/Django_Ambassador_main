from confluent_kafka import Consumer
import json, os, django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

from core.serializers import LinkSerializer

consumer = Consumer({
    'bootstrap.servers': 'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'T7FX5DMAEC4WND2R',
    'sasl.password': 'T05y4T3gGxkBrSNRTT/96//x3tKGrkhVv9mrAvC8dLIG6RKLwPwLZcDTNYDaOBuw',
    'sasl.mechanism': 'PLAIN',
    'group.id': 'myGroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['admin_topic'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    if msg.key() == b'"link_created"':
        try:
            link = json.loads(msg.value())

            print(link)

            serializer = LinkSerializer(data={
                'id': link['id'],
                'user_id': link['user_id'],
                'code': link['code'],
                'products': link['products']
            })
            serializer.is_valid(raise_exception=True)
            serializer.save()
        except:
            print("error happened")



consumer.close()
