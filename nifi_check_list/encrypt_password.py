import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from pathlib import Path
import logging
import click
from nifi_check_list.utils import utf8

sshHome = str(Path.home()) + "/.ssh/"
log = logging.getLogger("encrypt_password")
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def decryptSecret(encrypted: str) -> str:
    """
    Функция расшифровывает секрет:
    Параметр:
        encrypted - зашифвованный секрет в base46 кодировке.
    """
    log.debug("Загружаю имеющиеся сетрификаты с диска")
    with open(f'{sshHome}nifi_check_private_key.pem', "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    log.debug("Дешифрую пароль")
    decrypted = private_key.decrypt(
        base64.b64decode(encrypted),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return utf8(decrypted)


@click.command()
@click.option('--password', required=True, help='user password')
@click.option(
    '--log',
    'logLevel',
    default="ERROR",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    help='Log level. Default log level DEBUG')
def main(password, logLevel):
    """
    Функция выводит зашифрованный пароль для сохранения в конфигураторе.
    Параметр:
        password - шифруемый пароль
        logLevel - уровень логгирования для тестирования
    """
    logging.basicConfig(
        level=logLevel,
        format=FORMAT
    )
    log.debug("Генерирую сертификат для пароля")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    log.debug(f"Сохраняю сертификат в папки {sshHome}")
    with open(f'{sshHome}nifi_check_private_key.pem', 'wb') as f:
        f.write(private_pem)

    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(f'{sshHome}nifi_check_public_key.pem', 'wb') as f:
        f.write(public_pem)
    log.debug("Загружаю имеющиеся сетрификаты с диска")
    with open(f'{sshHome}nifi_check_private_key.pem', "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    with open(f'{sshHome}nifi_check_public_key.pem', "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    log.debug("Шифрую сетификатом пароль")

    encrypted = base64.b64encode(public_key.encrypt(
        password.encode('utf-8'),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    ))
    log.debug("Показываю зашифрованный пароль для пользователя")
    print(f'{utf8(encrypted)}')


if __name__ == "__main__":
    main()
