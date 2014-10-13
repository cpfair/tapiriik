from Crypto.PublicKey import RSA
key = RSA.generate(2048)

print("CREDENTIAL_STORAGE_PRIVATE_KEY = " + key.exportKey("DER").__repr__())
print("CREDENTIAL_STORAGE_PUBLIC_KEY = " + key.publickey().exportKey("DER").__repr__())
