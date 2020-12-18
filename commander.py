import boto3
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet

import fire, json
from envparse import env
import subprocess


class Commander(object):
    """
    Manages Poseidon
    """

    def __init__(self):
        env.read_envfile()
        self._aws_region = env.str("AWS_REGION")
        self._aws_access_key_id = env.str("SSM_AWS_ACCESS_KEY_ID")
        self._aws_secret_access_key = env.str("SSM_AWS_SECRET_ACCESS_KEY")
        self._kms_id = env.str("KMS_ID")


    def _get_secrets_from_json(self, env):
        with open('./secrets.json') as j:
            secrets = json.load(j)

        if env == 'local' or env == 'prod':
            return {**secrets['shared'], **secrets[env]}
        else:
            raise Exception("Invalid Env")


    def set_secrets(self, env):
        """
        Sets secrets on SSM for the environment

        env : string
            Variable used to set local or prod env
        """

        ssm = boto3.client('ssm',
                region_name=self._aws_region,
                aws_access_key_id = self._aws_access_key_id,
                aws_secret_access_key = self._aws_secret_access_key)

        secrets = self._get_secrets_from_json(env)

        for i in secrets:
            try:
                print("Setting {} for {} env".format(i, env))
                ssm.put_parameter(
                    Name="/{}/{}".format(env, i),
                    Description=i,
                    Value=secrets[i],
                    Type='SecureString',
                    KeyId=self._kms_id,
                    Overwrite=True
                )

            except ClientError as e:
                print(e.response['Error']['Code'])


    def get_fernet(self):
        """Generate Fernet Key"""
        fernet_key = Fernet.generate_key()
        print(fernet_key.decode())


    def garbage_collect(self):
        """ Cleanup all docker container images and exhaust."""
        print("Clean up all docker imagers and exhaust")
        subprocess.call('./docker-cleanup.sh', shell=True)

    def _validate_executor(self, executor):
        if executor not in ['sequential', 'local', 'celery']:
            raise ValueError("Invalid executor")

    def up(self, executor):
        """
        Bring up containers

        executor : string
            Which type of executor should we bring up. One of sequential, local or celery.

        """
        self._validate_executor(executor)
        print("Bring up containers for {} Executor".format(executor))
        subprocess_command = "docker-compose -f docker-compose-{}Executor.yml up -d"
        subprocess_command = subprocess_command.format(executor.capitalize())
        subprocess.call(subprocess_command, shell=True)



    def down(self, executor):
        """
        Bring down containers

        executor : string
            Which type of executor should we bring down. One of sequential, local or celery.

        """
        self._validate_executor(executor)
        print("Bring down containers for {} Executor".format(executor))
        subprocess_command = "docker-compose -f docker-compose-{}Executor.yml down"
        subprocess_command = subprocess_command.format(executor.capitalize())
        subprocess.call(subprocess_command, shell=True)

    def connect_container(self, container):
        """
        Connect to a running container's bash; Mimick operating env from compose.

        container : string
            Container name from `docker ps`

        """
        print("Connecting to {}".format(container))

        command = "docker exec -it {} /usr/local/airflow/entrypoint.sh /bin/bash".format(container)
        subprocess.call(command, shell=True)

    def connect_container_root(self, container):
        """
        Connect to a running container's bash; Mimick operating env from compose.

        container : string
            Container name from `docker ps`

        """
        print("Connecting to {}".format(container))

        command = "docker exec -it --user root {} secretly /usr/local/airflow/entrypoint.sh /bin/bash".format(container)
        subprocess.call(command, shell=True)



def main():
    fire.Fire(Commander(), name="commander")

if __name__ == '__main__':
    main()
