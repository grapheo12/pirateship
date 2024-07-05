#!/bin/bash

# Dependency: openssl jq

# Generates a CA certificates and a certificate used by all nodes.
# Node domain names: node$i.localhost which points to 127.0.0.1
# Also generates all the configs.
# Sample usage: sh tests/gen_local_config.sh configs 7 tests/local_template.json
# Do not commit the generated configs.

# Ported from: https://arminreiter.com/2022/01/create-your-own-certificate-authority-ca-using-openssl/

CANAME=Pft-Dev-RootCA

ROOTDIR=$1
NUMNODES=$2
TEMPLATE=$3

mkdir -p $ROOTDIR

# Copy the JSON template to rootdir, because we are going to cd into it and lose the path validity.
cp $TEMPLATE $ROOTDIR/template.json

cd $ROOTDIR

# Unencrypted priv key (don't use it in production)
openssl genrsa -out $CANAME.key 4096

# CA certificate
openssl req -x509 -new -nodes -key $CANAME.key -sha256 -days 1826 -out $CANAME.crt -subj '/CN=Pft Dev Root/C=IN/ST=West Bengal/L=Suri/O=Pft'


# The certificate will be issued to a fictitious entity "node"
# node0, ..., node(NUMNODES - 1) will be added to the SAN

# We are running a local setup
COMMON_DNS=localhost
COMMON_IP=127.0.0.1

openssl req -new -nodes -out $COMMON_DNS.csr -newkey rsa:4096 -keyout $COMMON_DNS.key -subj '/CN=Node/C=IN/ST=West Bengal/L=Suri/O=Pft'
cat > $COMMON_DNS.v3.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
EOF

for i in $(seq 1 $NUMNODES);
do
    echo "DNS.$i = node$i.$COMMON_DNS" >> $COMMON_DNS.v3.ext
done


for i in $(seq 1 $NUMNODES);
do
    echo "IP.$i = $COMMON_IP" >> $COMMON_DNS.v3.ext
done

openssl x509 -req -in $COMMON_DNS.csr -CA $CANAME.crt -CAkey $CANAME.key -CAcreateserial -out $COMMON_DNS.crt -days 730 -sha256 -extfile $COMMON_DNS.v3.ext

# Convert pkcs8 to pkcs1 format, otherwise rustls complains
openssl rsa -in $COMMON_DNS.key -out $COMMON_DNS.pkcs1.key

# Generate all Ed25519 keys used for signatures in the consensus protocol.
# All public keys will be pasted as "<node name> <public key>" in one file.
# This will be read by all servers to authenticate known peers.
# One extra key pair will be generated for client.
rm -rf signing_pub_keys.keylist
touch signing_pub_keys.keylist
for i in $(seq 1 $NUMNODES);
do
    openssl genpkey -algorithm ed25519 -out node$i\_signing_priv_key.pem
    echo "node$i $(openssl pkey -in node$i\_signing_priv_key.pem -pubout | grep -v 'PUBLIC KEY')" >> signing_pub_keys.keylist
done
openssl genpkey -algorithm ed25519 -out client_signing_priv_key.pem
echo "client $(openssl pkey -in client_signing_priv_key.pem -pubout | grep -v 'PUBLIC KEY')" >> signing_pub_keys.keylist

# Now generate all the JSON configs
PORT_PREFIX='300'

addrs="{"

for i in $(seq 1 $(( $NUMNODES - 1 )));
do
    addrs="${addrs} \"node$i\": {\"addr\": \"node$i.$COMMON_DNS:$PORT_PREFIX$i\", \"domain\": \"node$i.$COMMON_DNS\"},"
done

addrs="${addrs} \"node$NUMNODES\": {\"addr\": \"node$NUMNODES.$COMMON_DNS:$PORT_PREFIX$NUMNODES\", \"domain\": \"node$i.$COMMON_DNS\"} }"

BIND_ADDR_PREFIX="0.0.0.0:${PORT_PREFIX}"

for i in $(seq 1 $NUMNODES);
do
    cp template.json node$i.json
    privkey_fname=$(pwd)/node$i\_signing_priv_key.pem
    jq ".net_config.nodes = $addrs |\
    .net_config.tls_cert_path = \"$(pwd)/$COMMON_DNS.crt\" |\
    .net_config.tls_key_path = \"$(pwd)/$COMMON_DNS.pkcs1.key\" |\
    .net_config.tls_root_ca_cert_path = \"$(pwd)/$CANAME.crt\" |\
    .net_config.name = \"node$i\" |\
    .net_config.addr = \"$BIND_ADDR_PREFIX$i\" |\
    .rpc_config.allowed_keylist_path = \"$(pwd)/signing_pub_keys.keylist\" |\
    .rpc_config.signing_priv_key_path = \"$privkey_fname\"" node$i.json > tmp.json
    cp tmp.json node$i.json
done
rm tmp.json
rm template.json


