import requests
import certifi
import sys

try:
    requests.get('https://any-website-protected-by-your-custom-root-ca')
    print('Certificate already added to the certifi store')
    sys.exit(0)
except requests.exceptions.SSLError as err:
    print('SSL Error. Adding custom certs to Certifi store...')
    customca = requests.get('http://place-to-download-your-root-ca-pem.file').content
    cafile = certifi.where()
    with open(cafile, 'ab') as outfile:
        outfile.write(b'\n')
        outfile.write(customca)

try:
    requests.get('https://any-website-protected-by-your-custom-root-ca-to-validate-the-certificate')
    print('Successfully added certificate to the certifi store')
    sys.exit(0)
except requests.exceptions.SSLError as err:
    print('Failed to add certificate to the certifi store')
    sys.exit(1)