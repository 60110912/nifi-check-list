import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from pathlib import Path
import logging
from nifi_check_list.utils import utf8


sshHome = str(Path.home()) + "/.ssh/"
log = logging.getLogger("encrypt")


def decryptSecret(encrypted: str) -> str:
    """
    Функция расшифровывает секрет:
    Параметр:
        encrypted - зашифвованный секрет в base46 кодировке.
    """
    log.info("Загружаю имеющиеся сетрификаты с диска")
    with open(f'{sshHome}nifi_check_private_key.pem', "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    log.info("Дешифрую пароль")
    decrypted = private_key.decrypt(
        base64.b64decode(encrypted),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return utf8(decrypted)
