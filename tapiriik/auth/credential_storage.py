from Crypto.Cipher import AES
from Crypto import Random
import hashlib
from tapiriik.settings import CREDENTIAL_STORAGE_KEY

#### note about tapiriik and credential storage ####
# Some services require a username and password for every action - so they need to be stored in recoverable form
# (namely: Garmin Connect's current "API")
# I've done my best to mitigate the risk that these credentials ever be compromised, but the risk can never be eliminated
# If you're not comfortable with it, you can opt to not have your credentials stored, instead entering them on every sync

class CredentialStore:
    def GenerateIV():
        return Random.new().read(AES.block_size)

    def Encrypt(cred):
        iv = CredentialStore.GenerateIV();
        cipher = AES.new(CREDENTIAL_STORAGE_KEY, AES.MODE_CFB, iv)
        data = cipher.encrypt(cred.encode("UTF-8"))
        return [iv, data]

    def Decrypt(data):
        iv = data[0]
        data = data[1]
        cipher = AES.new(CREDENTIAL_STORAGE_KEY, AES.MODE_CFB, iv)
        cred = cipher.decrypt(data).decode("UTF-8")
        return cred
