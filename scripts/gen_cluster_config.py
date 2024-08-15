from typing import Dict, Tuple, OrderedDict
import click
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


def gen_root_ca_key(caname: str) -> rsa.RSAPrivateKey:
    root_ca_key = rsa.generate_private_key(
        public_exponent=65537,      # This is a fixed constant
        key_size=4096
    )
    pem = root_ca_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(caname + ROOT_KEY_SUFFIX, "wb") as f:
        f.write(pem)

    return root_ca_key

def read_root_ca_key(caname: str) -> rsa.RSAPrivateKey:
    with open(caname + ROOT_KEY_SUFFIX, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
        assert(isinstance(key, rsa.RSAPrivateKey))
        return key
        
        

def gen_root_cert(key: rsa.RSAPrivateKey, caname: str) -> x509.Certificate:
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
    with open(caname + ROOT_CERT_SUFFIX, "wb") as f:
        f.write(pem)

    return root_cert


def gen_cert_priv_key(node: str) -> rsa.RSAPrivateKey:
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096
    )
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(node + TLS_PRIVKEY_SUFFIX, "wb") as f:
        f.write(pem)

    return key


def gen_csr(node: str, domain: str, key: rsa.RSAPrivateKey) -> x509.CertificateSigningRequest:
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

    with open(node + CSR_SUFFIX, "wb") as f:
        f.write(pem)

    return csr


def gen_node_certificate(node: str, csr: x509.CertificateSigningRequest, key: rsa.RSAPrivateKey, caname: str) -> x509.Certificate:
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
    with open(node + TLS_CERT_SUFFIX, "wb") as f:
        f.write(pem)

    return cert
    

def gen_signing_keypair(node) -> Tuple[ed25519.Ed25519PrivateKey, ed25519.Ed25519PublicKey]:
    priv_key = ed25519.Ed25519PrivateKey.generate()
    pem = priv_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(node + SIGN_PRIVKEY_SUFFIX, "wb") as f:
        f.write(pem)

    return priv_key, priv_key.public_key()


def gen_keys_and_certs(nodelist: Dict[str, Tuple[str, str]], caname: str, client_cnt: int, gen_root_ca=True, gen_combined_keylist=True):
    print(nodelist)
    if gen_root_ca:
        print("Generating key for root CA")
        root_ca_key = gen_root_ca_key(caname)
        print("Generating root CA certificate")
        root_ca_cert = gen_root_cert(root_ca_key, caname)
    else:
        print("Fetching root CA key")
        root_ca_key = read_root_ca_key(caname)

    print("Generating certificate private key for each node")
    cert_keys = {node: gen_cert_priv_key(node) for node in nodelist.keys()}
    print("Generating certificate signing requests for each node")
    csrs = {node: gen_csr(node, domain, cert_keys[node]) for node, (_, domain) in nodelist.items()}
    print("Generating node certificates")
    node_certs = {node: gen_node_certificate(node, csr, root_ca_key, caname) for node, csr in csrs.items()}

    print("Generating signing keypairs")
    keypairs = {node: gen_signing_keypair(node) for node in nodelist}
    keypairs.update(
        {"client" + str(i): gen_signing_keypair("client" + str(i)) for i in range(1, client_cnt + 1)})
    
    if gen_combined_keylist:
        keypairs.update(
            {"controller": gen_signing_keypair("controller")})
        print("Generating public key list")
        with open(PUB_KEYLIST_NAME, "w") as f:
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
            with open(f"{node}{LEARNER_SUFFIX}", "w") as f:
                print(node, addr, domain, pubk_line.decode(), file=f)

# All gen_*_nodelist functions return: dict(node -> (ip, domain name)) and number of clients
def gen_cluster_nodelist(ip_list, domain_suffix, cnt_start=0) -> Tuple[OrderedDict[str, Tuple[str, str]], int]:
    if ip_list == "/dev/null":
        raise Exception("Ip list must be provided when using cluster mode")
    
    nodelist = collections.OrderedDict()
    node_cnt = cnt_start
    client_cnt = 0
    with open(ip_list) as f:
        for line in f.readlines():
            # Terraform generates VM names as `nodepool_vm0` and `clientpool_vm0`.
            # IP list output by terraform must be of the form:
            # nodepool_vm0 <private ip address>
            if line.startswith("node"):
                node_cnt += 1
                ip = line.split()[1]
                nodelist["node" + str(node_cnt)] = (ip.strip(), "node" + str(node_cnt) + domain_suffix)
            
            if line.startswith("client"):
                client_cnt += 1

    return (nodelist, client_cnt)


def gen_local_nodelist(num_nodes: int) -> Tuple[OrderedDict[str, Tuple[str, str]], int]:
    if num_nodes <= 0:
        raise Exception("Number of nodes must be provided when using local mode")
    nodelist = OrderedDict()
    for i in range(1, num_nodes + 1):
        node_name = "node" + str(i)
        nodelist[node_name] = ("127.0.0.1", node_name + ".localhost")

    return (nodelist, 1)

# Node's binding address will always be 0.0.0.0:(3000 + index)
# This makes sure that no matter what environment I am in,
# Ports will not clash with one another.
# However, for a kubernetes-style deployment,
# it may make more sense to have all nodes listen on same port.
def gen_node_config(i, node, tmpl, caname, root_path):
    tmpl["net_config"]["tls_cert_path"] = os.path.join(root_path, node + TLS_CERT_SUFFIX)
    tmpl["net_config"]["tls_key_path"] = os.path.join(root_path, node + TLS_PRIVKEY_SUFFIX)
    tmpl["net_config"]["tls_root_ca_cert_path"] = os.path.join(root_path, caname + ROOT_CERT_SUFFIX)
    tmpl["net_config"]["name"] = node
    tmpl["net_config"]["addr"] = "0.0.0.0:" + str(3000 + i + 1)
    tmpl["rpc_config"]["allowed_keylist_path"] = os.path.join(root_path, PUB_KEYLIST_NAME)
    tmpl["rpc_config"]["signing_priv_key_path"] = os.path.join(root_path, node + SIGN_PRIVKEY_SUFFIX)

    with open(node + CONFIG_SUFFIX, "w") as f:
        json.dump(tmpl, f, indent=4)

def gen_node_configs(nodelist: OrderedDict[str, Tuple[str, str]], tmpl: Dict, caname, root_path):
    tmpl["net_config"]["nodes"] = {node: {"addr": addr + ":" + str(3000 + i + 1), "domain": domain} for i, (node, (addr, domain)) in enumerate(nodelist.items())}
    tmpl["consensus_config"]["node_list"] = list(nodelist.keys())
    for i, node in enumerate(nodelist.keys()):
        gen_node_config(i, node, deepcopy(tmpl), caname, root_path)

def gen_client_config(i, root_path, tmpl):
    client = "client" + str(i)
    tmpl["net_config"]["name"] = client
    tmpl["rpc_config"]["signing_priv_key_path"] = os.path.join(root_path, client + SIGN_PRIVKEY_SUFFIX)

    with open(client + CONFIG_SUFFIX, "w") as f:
        json.dump(tmpl, f, indent=4)

def gen_controller_config(root_path, tmpl):
    tmpl["net_config"]["name"] = "controller"
    tmpl["rpc_config"]["signing_priv_key_path"] = os.path.join(root_path, "controller" + SIGN_PRIVKEY_SUFFIX)

    with open("controller" + CONFIG_SUFFIX, "w") as f:
        json.dump(tmpl, f, indent=4)

def gen_client_configs(num_clients: int, nodelist: OrderedDict[str, Tuple[str, str]], tmpl: Dict, caname, root_path):
    tmpl["net_config"]["nodes"] = {node: {"addr": addr + ":" + str(3000 + i + 1), "domain": domain} for i, (node, (addr, domain)) in enumerate(nodelist.items())}
    tmpl["net_config"]["tls_root_ca_cert_path"] = os.path.join(root_path, caname + ROOT_CERT_SUFFIX)
    
    for i in range(1, num_clients + 1):
        gen_client_config(i, root_path, deepcopy(tmpl))


def gen_config(outdir, mode, node_template, client_template, ip_list, num_nodes):
    print("Reading templates")
    node_template = json.load(open(node_template))
    client_template = json.load(open(client_template))

    nodelist = OrderedDict()
    client_cnt = 0
    if mode == "local":
        nodelist, client_cnt = gen_local_nodelist(num_nodes)
    elif mode == "remoteclient":
        raise NotImplementedError
    elif mode == "cluster":
        nodelist, client_cnt = gen_cluster_nodelist(ip_list, ".pft.org")
    else:
        raise NotImplementedError
    
    print("Number of nodes:", len(nodelist))
    print("Number of clients:", client_cnt)

    print("Creating directory")
    os.makedirs(outdir, exist_ok=True)
    pwd = os.getcwd()
    os.chdir(outdir)

    gen_keys_and_certs(nodelist, DEFAULT_CA_NAME, client_cnt)

    print("Generating Node configs")
    gen_node_configs(nodelist, node_template, DEFAULT_CA_NAME, outdir)

    print("Generating Client configs")
    gen_client_configs(client_cnt, nodelist, client_template, DEFAULT_CA_NAME, outdir)

    print("Generating Controller config")
    gen_controller_config(outdir, client_template)

    os.chdir(pwd)



@click.command()
@click.option(
    "-o", "--outdir", required=True,
    help="Output directory",
    type=click.Path(file_okay=False, resolve_path=False))
@click.option(
    "-m", "--mode", required=True,
    type=click.Choice(["local", "remoteclient", "cluster"]),
    default="cluster",
    help="Type of config"
)
@click.option(
    "-nt", "--node_template", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    help="JSON template for node config"
)
@click.option(
    "-ct", "--client_template", required=True,
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    help="JSON template for client config"
)
@click.option(
    "-ips", "--ip_list",
    default="/dev/null",
    help="File with list of node names and IP addresses to be used with cluster config",
    type=click.Path(exists=True, file_okay=True, resolve_path=True)
)
@click.option(
    "-n", "--num_nodes",
    default=-1,
    help="Number of nodes",
    type=click.INT
)
def main(outdir, mode, node_template, client_template, ip_list, num_nodes):
    gen_config(outdir, mode, node_template, client_template, ip_list, num_nodes)


if __name__ == "__main__":
    main()
