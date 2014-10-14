from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA
from Crypto import Random
from tapiriik.settings import CREDENTIAL_STORAGE_PUBLIC_KEY, CREDENTIAL_STORAGE_PRIVATE_KEY
import copy

#### note about tapiriik and credential storage ####
# Some services require a username and password for every action - so they need to be stored in recoverable form
# (namely: Garmin Connect's current "API")
# I've done my best to mitigate the risk that these credentials ever be compromised, but the risk can never be eliminated
# If you're not comfortable with it, you can opt to not have your credentials stored, instead entering them on every sync

class CredentialStore:
    def Init():
        _key = RSA.importKey(CREDENTIAL_STORAGE_PRIVATE_KEY if CREDENTIAL_STORAGE_PRIVATE_KEY else CREDENTIAL_STORAGE_PUBLIC_KEY)
        CredentialStore._cipher = PKCS1_OAEP.new(_key)

    def Encrypt(cred):
        data = CredentialStore._cipher.encrypt(cred.encode("UTF-8"))

        # We store the plaintext credential so that the web server can use it later during the same request.
        # After the request terminates, it'll be GC'd more-or-less as quickly as the incoming request data itself.
        return ShadowedCredential(data, cred)

    def Decrypt(data):
        # Check if we encrypted the data in the same session, and use that instead (we might not have the private key).
        if isinstance(data, ShadowedCredential):
            return data.plaintext

        # I kind of doubt anyone could get away with a timing attack on the sycnhronization workers.
        # But, dear comment-reader, I'm sure you're now contemplating the possibilities...
        # So PKCS#1 OAEP it is.
        return CredentialStore._cipher.decrypt(data).decode("UTF-8")

    def FlattenShadowedCredentials(auth_dict):
        flat_auth_dict = copy.deepcopy(auth_dict)
        for k, v in auth_dict.items():
            if isinstance(v, ShadowedCredential):
                flat_auth_dict[k] = v.ciphertext
        return flat_auth_dict

class ShadowedCredential:
    def __init__(self, ciphertext, plaintext):
        self.ciphertext = ciphertext
        self.plaintext = plaintext

CredentialStore.Init()
