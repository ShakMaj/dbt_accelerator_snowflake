import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# The private key (unencrypted) in PEM format
private_key = b'''-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDIZ+9P+SYgb5yH
F94NJuP8he8t4ZX6N1ydNeTMZxR/5AOM4/vgeUQlViYsdAWA6dNuI3DaIyG+Lr/C
uc8h1jNehR+YKE5toIaiFntANla6kk7QTit+yM+IRVyqwYHZ6rWp58p0hK1ijHld
CZQvjS2SPiZIbyIApIHaG7k82/aCzUhqwpM9UfwGdGQMy4VUja6uIyDgVjkg03nK
kVWcq5ZHJwOU3WcAieO5ajBl4CVI3OmMXFfUiFWfitm6jD6X3vDt9NbibMlYONLA
Bo5PIwBglWskp9zmesXCcB0jPklm+GrQMBF7Y6cnDJgPrWO9fhFULKZf7smSsMPx
Gj8f7TT1AgMBAAECggEAAXRpTX16sNHHSpBZx8pKoTh8+Cp1eRjQZYddH2ctc0JZ
20yet4rfbA2TK6l0d/ci+jJaLBcf8y9xSRoKc+/bLSFXAlQW0aexHgHmUGJxOboY
xdreE2qbszEspssEZoGeXjuEugf1Zula7DYMgqswPKD5sGT4BRY+lXtwpqP2S33w
Qd09Mb4Imwe2B2KTm2IkS0N3qStcN7gkfHvhzNLSW8Oog6GZYSYsRIws+JnTUtFb
VSPLJL8ByJyjzoQVSzHF2fqD4eXFfy4+UL2tb3pLT+5+rIpzV7wv16e8E08ouIjH
VaGMunXKuApazdweLE61N3pPIQLoxhQ2qOfoFG5QRwKBgQDtUCXlxrdFe0Oke68m
liER67Y6cCDhzr8tZYpCTqyZrRgFTR+IyF6aKmg4UTNCvUZFZ0DSRiGBpsWmbdpQ
HIJDNnZEVwYCAq4KqYBfOuIQDQB0aRndKk3gojTu75zsQKm5GX+8COPLaACXxxG1
GaY0cZxlo+DvLZOskvRPG9NREwKBgQDYL8yNnF4O0D1VibuXsqP6AAIg1Mwxt8K0
Z/a6G0CoYvQQk45T/vtdF0SiYUE9YLmP1I6qEhNji0Z0eZMByP/7x3gYagHKi9Hi
/3DOCM+FqVPTwlPV+R+BDJ1Dg59/T48bs5kP/bm0YjWeippoqumNHVnuFnie8FMF
QLf+XR0q1wKBgQCEpYAJZ4GZfMTac6f7UX86nR5Gm9ZFYWLK5S7a0U74tx/uFqXB
tv3p/jozAHA+pb1ZndVFs52W8rkXxd95V/YXy6FfoaTiCs1teEEg+bDSd/IBH54t
WTAel4wu4hA8nghARlh3qiBp2cA7FjJ3n9afCcH5rhhUmI0qsRmN/mensQKBgQCm
//WiBwLZqp9WrnkNbGAjRVJfjkLWvT9ZIV6E0XyXFym2jGXMGRKQDONmFH7TLR7r
LqWuVxxHL1WTsD6RiHXGR4ZMRcdwcaYHFpBLeJ7mf7FTya/9gN6HT/lEKC9Tp1/D
ZG9+3N9RqvQErsksxap46g2IUC+Jh9h6HE6F3eQkLQKBgHhgjMCrPQf6CKoZd5SY
ne+MUyDr0NiNWxx8VnQq22fdfJAwGuy8vE5GEtu2vvST81jaegk+7Z2Z5teP+tQX
vkS7tB0xOBwDYrGDuntmadmqulhUQlap8sKvkdpJ5mZzpiJhN98V6r+6GG2hVtkQ
IzI3kuyoZhtE6rc2iLhzx/JB
-----END PRIVATE KEY-----'''

# Load the private key (unencrypted)
p_key = serialization.load_pem_private_key(
    private_key,
    password=None,  # Pass None for unencrypted key
    backend=default_backend()
)

# Convert private key to DER format
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Connect to Snowflake using the private key
ctx = snowflake.connector.connect(

    private_key=private_key,
    account = 'wla98892-wfld_enterprise' ,
    user = 'SVC-ADF-SF@WESTFIELDGRP.ONMICROSOFT.COM'
)

print('connected to snowflake')
