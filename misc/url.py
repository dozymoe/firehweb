from urllib.parse import urlunsplit

def url_from_dict(data, prefix='', scheme='', host='', port=None, path='',
        host_field_name='host'):

    port = data.get(prefix + 'port', port)
    if port:
        netloc = '%s:%i' % (data.get(prefix + host_field_name, host), port)
    else:
        netloc = data.get(prefix + host_field_name, host)

    return urlunsplit((
        data.get(prefix + 'scheme', scheme),
        netloc,
        data.get(prefix + 'path', path), '', ''))


def connect_url_from_dict(*args, **kwargs):
    return url_from_dict(host_field_name='connect', *args, **kwargs)


def listen_url_from_dict(*args, **kwargs):
    return url_from_dict(host_field_name='listen', *args, **kwargs)
