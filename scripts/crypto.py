# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

from typing import Dict, Tuple, OrderedDict
from cryptography.hazmat.primitives.asymmetric import rsa, ed25519
from cryptography.hazmat.primitives import hashes
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography import x509
from cryptography.hazmat.primitives import serialization
import datetime
import os
import json
from copy import deepcopy
import collections

ROOT_KEY_SUFFIX = "_root_key.pem"
ROOT_CERT_SUFFIX = "_root_cert.pem"
TLS_CERT_SUFFIX = "_tls_cert.pem"
TLS_PRIVKEY_SUFFIX = "_tls_privkey.pem"
CSR_SUFFIX = "_csr.pem"
PUB_KEYLIST_NAME = "signing_pub_keys.keylist"
SIGN_PRIVKEY_SUFFIX = "_signing_privkey.pem"
CONFIG_SUFFIX = "_config.json"
LEARNER_SUFFIX = "_learner_info.txt"
DEFAULT_CA_NAME = "Pft"


def gen_root_ca_key(caname: str, config_dir) -> rsa.RSAPrivateKey:
    root_ca_key = rsa.generate_private_key(
        public_exponent=65537,      # This is a fixed constant
        key_size=4096
    )
    pem = root_ca_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(os.path.join(config_dir, caname + ROOT_KEY_SUFFIX), "wb") as f:
        f.write(pem)

    return root_ca_key

def read_root_ca_key(caname: str, config_dir) -> rsa.RSAPrivateKey:
    with open(os.path.join(config_dir, caname + ROOT_KEY_SUFFIX), "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
        assert(isinstance(key, rsa.RSAPrivateKey))
        return key
        
        

def gen_root_cert(key: rsa.RSAPrivateKey, caname: str, config_dir) -> x509.Certificate:
    issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "IN"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "West Bengal"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Suri"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, caname),
        x509.NameAttribute(NameOID.COMMON_NAME, caname),
    ])
    root_cert = x509.CertificateBuilder()\
        .issuer_name(issuer)\
        .subject_name(issuer)\
        .public_key(key.public_key())\
        .serial_number(x509.random_serial_number())\
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))\
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))\
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True
        ).add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                decipher_only=False,
                encipher_only=False
            ),
            critical=True
        ).add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False
        ).sign(key, hashes.SHA256())
    
    pem = root_cert.public_bytes(serialization.Encoding.PEM)
    with open(os.path.join(config_dir, caname + ROOT_CERT_SUFFIX), "wb") as f:
        f.write(pem)

    return root_cert


def gen_cert_priv_key(node: str, config_dir) -> rsa.RSAPrivateKey:
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096
    )
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(os.path.join(config_dir, node + TLS_PRIVKEY_SUFFIX), "wb") as f:
        f.write(pem)

    return key


def gen_csr(node: str, domain: str, key: rsa.RSAPrivateKey, config_dir) -> x509.CertificateSigningRequest:
    csr = x509.CertificateSigningRequestBuilder()\
        .subject_name(x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "IN"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "West Bengal"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Suri"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, domain),
            x509.NameAttribute(NameOID.COMMON_NAME, node),
        ])).add_extension(x509.SubjectAlternativeName([
            x509.DNSName(domain)
        ]), critical=False)\
        .sign(key, hashes.SHA256())
    
    pem = csr.public_bytes(serialization.Encoding.PEM)

    with open(os.path.join(config_dir, node + CSR_SUFFIX), "wb") as f:
        f.write(pem)

    return csr


def gen_node_certificate(node: str, csr: x509.CertificateSigningRequest, key: rsa.RSAPrivateKey, caname: str, config_dir) -> x509.Certificate:
    issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "IN"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "West Bengal"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Suri"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, caname),
        x509.NameAttribute(NameOID.COMMON_NAME, caname),
    ])
    cert = x509.CertificateBuilder()\
        .issuer_name(issuer)\
        .subject_name(csr.subject)\
        .public_key(csr.public_key())\
        .serial_number(x509.random_serial_number())\
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))\
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))\
        .add_extension(x509.KeyUsage(
            digital_signature=True,
            content_commitment=False,
            key_encipherment=True,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=True,
            encipher_only=False,
            decipher_only=False,
        ), critical=True,)\
        .add_extension(x509.ExtendedKeyUsage([
            ExtendedKeyUsageOID.CLIENT_AUTH,
            ExtendedKeyUsageOID.SERVER_AUTH,
        ]), critical=False)\
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(csr.public_key()),
            critical=False)\
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True)\
        .add_extension(
            csr.extensions.get_extension_for_class(x509.SubjectAlternativeName).value,
            critical=True)
    

    cert = cert.sign(key, hashes.SHA256())
    pem = cert.public_bytes(serialization.Encoding.PEM)
    with open(os.path.join(config_dir, node + TLS_CERT_SUFFIX), "wb") as f:
        f.write(pem)

    return cert
    

def gen_signing_keypair(node, config_dir) -> Tuple[ed25519.Ed25519PrivateKey, ed25519.Ed25519PublicKey]:
    priv_key = ed25519.Ed25519PrivateKey.generate()
    pem = priv_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(os.path.join(config_dir, node + SIGN_PRIVKEY_SUFFIX), "wb") as f:
        f.write(pem)

    return priv_key, priv_key.public_key()


def gen_keys_and_certs(nodelist: Dict[str, Tuple[str, str]], caname: str, client_cnt: int, config_dir, gen_root_ca=True, gen_combined_keylist=True):
    print(nodelist)
    if gen_root_ca:
        print("Generating key for root CA")
        root_ca_key = gen_root_ca_key(caname, config_dir)
        print("Generating root CA certificate")
        root_ca_cert = gen_root_cert(root_ca_key, caname, config_dir)
    else:
        print("Fetching root CA key")
        root_ca_key = read_root_ca_key(caname, config_dir)

    print("Generating certificate private key for each node")
    cert_keys = {node: gen_cert_priv_key(node, config_dir) for node in nodelist.keys()}
    print("Generating certificate signing requests for each node")
    csrs = {node: gen_csr(node, domain, cert_keys[node], config_dir) for node, (_, domain) in nodelist.items()}
    print("Generating node certificates")
    node_certs = {node: gen_node_certificate(node, csr, root_ca_key, caname, config_dir) for node, csr in csrs.items()}

    print("Generating signing keypairs")
    keypairs = {node: gen_signing_keypair(node, config_dir) for node in nodelist}
    keypairs.update(
        {"client" + str(i): gen_signing_keypair("client" + str(i), config_dir) for i in range(1, client_cnt + 1)})
    
    if gen_combined_keylist:
        keypairs.update(
            {"controller": gen_signing_keypair("controller", config_dir)})
        print("Generating public key list")
        with open(os.path.join(config_dir, PUB_KEYLIST_NAME), "w") as f:
            for node, (_, pubk) in keypairs.items():
                pubk_pem = pubk.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                )

                pubk_line = pubk_pem.splitlines()[1]
                print(node, pubk_line.decode(), file=f)
    else:
        print("Generating Learner Info oneliners")
        for node, (addr, domain) in nodelist.items():
            _, pubk = keypairs[node]
            pubk_pem = pubk.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            pubk_line = pubk_pem.splitlines()[1]
            with open(os.path.join(config_dir, f"{node}{LEARNER_SUFFIX}"), "w") as f:
                print(node, addr, domain, pubk_line.decode(), file=f)

    return list(keypairs.keys())