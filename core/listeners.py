from .serializers import LinkSerializer


def link_created(data):
    print(data)

    serializer = LinkSerializer(data={
        'id': data['id'],
        'user_id': data['user_id'],
        'code': data['code'],
        'products': data['products']
    })
    serializer.is_valid(raise_exception=True)
    serializer.save()
