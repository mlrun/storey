from os import environ

has_v3io_creds = environ.get('V3IO_API') and environ.get('V3IO_ACCESS_KEY')
