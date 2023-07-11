import aerospike

config = {
    'hosts': [
        ( 'example85', 30000 )
    ],
    'policies': {
        'timeout': 1000 # milliseconds
    }
}

client = aerospike.client(config)

