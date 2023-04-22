#!/bin/bash
mkdir -p tls
cd tls
openssl genrsa -aes256 -passout pass:abcd1234 -out ca-key.pem 2048
openssl req -x509 -new -nodes -extensions v3_ca -key ca-key.pem -days 1024 -out ca.pem -sha512 -subj "/C=DE/ST=NRW/L=Kerpen/O=Neunhoeffer/OU=Max/CN=Max Neunhoeffer/emailAddress=max@arangodb.de/" -passin pass:abcd1234
openssl genrsa -passout pass:abcd1234 -out key.pem 2048
cat > ssl.conf <<EOF
[req]
prompt = no
distinguished_name = myself

[myself]
C = de
ST = NRW
L = Kerpen
O = Neunhoeffer
OU = Max
CN = xeo.9hoeffer.de

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = xeo.9hoeffer.de
DNS.3 = 127.0.0.1
EOF

openssl req -new -key key.pem -out key-csr.pem -sha512 -config ssl.conf -subj "/C=DE/ST=NRW/L=Kerpen/O=Neunhoeffer/OU=Labor/CN=xeo.9hoeffer.de/"

openssl x509 -req -in key-csr.pem -CA ca.pem -days 3650 -CAkey ca-key.pem -out cert.pem -extensions req_ext -extfile ssl.conf -passin pass:abcd1234 -CAcreateserial

cat ca.pem cert.pem key.pem > keyfile.pem

# Client authentication:

openssl genrsa -aes256 -passout pass:abcd1234 -out authca-key.pem 2048
openssl req -x509 -new -nodes -extensions v3_ca -key authca-key.pem -days 1024 -out authca.pem -sha512 -subj "/C=DE/ST=NRW/L=Kerpen/O=Neunhoeffer/OU=Max/CN=Max Neunhoeffer/emailAddress=max@arangodb.de/" -passin pass:abcd1234

openssl genrsa -passout pass:abcd1234 -out client-key.pem 2048
openssl req -new -passin pass:abcd1234 -key client-key.pem -out client-csr.pem -subj "/O=ArangoDB/CN=Max/"

cat > ssl.conf <<EOF
[req]
prompt = no
distinguished_name = myself

[myself]
O = ArangoDB
CN = ArangoDB

[client]
keyUsage = critical,Digital Signature,Key Encipherment
extendedKeyUsage = @key_usage
basicConstraints = critical,CA:FALSE

[key_usage]
1 = Any Extended Key Usage
2 = TLS Web Client Authentication
EOF

openssl x509 -req -passin pass:abcd1234 -in client-csr.pem -CA authca.pem -CAkey authca-key.pem -set_serial 101 -extensions client -days 365 -outform PEM -out client-cert.pem -extfile ssl.conf

openssl pkcs12 -export -inkey client-key.pem -in client-cert.pem -out client.p12 -passout pass:abcd1234

rm key-csr.pem client-csr.pem ssl.conf

cat cert.pem ca.pem > chain.pem
