#!/bin/bash
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

# Now generate all the JSON configs
PORT_PREFIX='300'

addrs="{"

for i in $(seq 1 $(( $NUMNODES - 1 )));
do
    addrs="${addrs} \"node$i\": {\"addr\": \"node$i.$COMMON_DNS:$PORT_PREFIX$i\"},"
done

addrs="${addrs} \"node$NUMNODES\": {\"addr\": \"node$NUMNODES.$COMMON_DNS:$PORT_PREFIX$NUMNODES\"} }"

BIND_ADDR_PREFIX="0.0.0.0:${PORT_PREFIX}"

for i in $(seq 1 $NUMNODES);
do
    cp template.json node$i.json
    # Copy to a tmp file, copy back the tmp file
    # Doing this repetatively is not efficient.
    # But these files won't be bigger than few KiBs.

    jq ".net_config.nodes = $addrs" node$i.json > tmp.json
    cp tmp.json node$i.json
    
    jq ".net_config.cert_path = \"$(pwd)/$COMMON_DNS.crt\"" node$i.json > tmp.json
    cp tmp.json node$i.json
    
    jq ".net_config.root_ca_cert_path = \"$(pwd)/$CANAME.crt\"" node$i.json > tmp.json
    cp tmp.json node$i.json

    jq ".net_config.name = \"node$i\"" node$i.json > tmp.json
    cp tmp.json node$i.json

    jq ".net_config.addr = \"$BIND_ADDR_PREFIX$i\"" node$i.json > tmp.json
    cp tmp.json node$i.json
done
rm tmp.json
rm template.json


