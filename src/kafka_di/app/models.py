import enum
from dataclasses import dataclass


class SecurityProtocols(enum.Enum):
    NONE = 'NONE'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'


@dataclass
class Config:
    bootstrap_servers: str
    group_id: str
    sasl_username: str | None = None
    sasl_password: str | None = None
    security_protocol: SecurityProtocols = SecurityProtocols.NONE
    sasl_mechanisms: str | None = None

    def serialize(self) -> dict:
        configs = self.serialize_producer()
        configs['group.id'] = self.group_id
        return configs

    def serialize_producer(self) -> dict:
        configs = {
            'bootstrap.servers': self.bootstrap_servers,
        }

        if self.security_protocol == SecurityProtocols.SASL_PLAINTEXT:
            configs['security.protocol'] = self.security_protocol.value
            configs['sasl.mechanisms'] = self.sasl_mechanisms
            configs['sasl.username'] = self.sasl_username
            configs['sasl.password'] = self.sasl_password

        elif self.security_protocol == SecurityProtocols.SASL_SSL:
            configs['security.protocol'] = self.security_protocol.value
            configs['sasl.mechanisms'] = self.sasl_mechanisms
            configs['sasl.username'] = self.sasl_username
            configs['sasl.password'] = self.sasl_password
            configs['ssl.ca.location'] = '/var/run/kafka/ca.crt'

        return configs
