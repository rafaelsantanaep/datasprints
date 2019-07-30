import boto3
import pandas as pd
import json
import datetime
from time import sleep
import getpass

first_piece = """
A primeira informação necessária é sua AWS_ACCESS_KEY_ID. Para obter uma KEY é necessário que você tenha criado um IAM_USER que pode ser criado através desse site: `https://console.aws.amazon.com/iam/home?region=us-west-2#/users`. Para a utilização dessa KEY em conjunto com o Redshift, é necessário que sejam atribuidas a esse usuário, as permissões de leitura e writing do Redshift. Também será necessário que esse usuário tenha permissões para alterar permissões da Amazon, dessa forma, ele precisará ter Administrator Access.
"""


class ConfiguringCloud:
    def __init__(self):
        self.aws_key = ""
        self.aws_secret = ""
        self.role_arn = "data-sprints-test-role"
        self.iam_role = ""
        self.end_point = ""
        self.db_name = "dev"
        self.user = "awsuser"
        self.password = "Passw0rd*"
        self.port = "5439"
        self.dwh_cluster_type="multi-node"
        self.dwh_num_nodes=4
        self.dwh_node_type="dc2.large"

    def filling_parameters(self):
        
        print("Vamos iniciar o processo de criação do seu cluster")
        print("Para tanto, será necessário algumas informações sobre a sua conta!")
        print(first_piece)

        self.aws_key = getpass.getpass(prompt="AWS_ACCESS_KEY_ID: ")

        while len(self.aws_key) != 20:
            print("O Valor fornecido não possui o formato correto! A AWS_ACCESS_KEY deve conter 20 caracteres.")
            self.aws_key = getpass.getpass(prompt="AWS_ACCESS_KEY_ID: ")

        print()
        print("A segunda informações necessária é sua AWS_ACCESS_SECRET_ID")
        print("Essa informação é obtida em conjunto com a AWS_ACCESS_SECRET_KEY")
        self.aws_secret = getpass.getpass(prompt="AWS_SECRET_ACCESS_KEY: ")
        
        while len(self.aws_secret) != 40:
            print("O Valor fornecido não possui o formato correto! A AWS_ACCESS_SECRET_KEY deve conter 40 caracteres.")
            self.aws_secret = getpass.getpass(prompt="AWS_SECRET_ACCESS_KEY: ")

    def printing_parameters(self):
        key = f"{self.aws_key[:4]}" + '*'*12 + f"{self.aws_key[-4:]}"
        secret = f"{self.aws_secret[:4]}" + '*'*32 + f"{self.aws_secret[-4:]}"
        print()
        print('='*65)
        print("AWS_ACCESS_KEY       |", key, ' ' * 18)
        print("-"*65)
        print("AWS_SECRET_ACCESS_KEY|", secret)
        print("-"*65)

    def creating_cluster(self):
        redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=self.aws_key,
                       aws_secret_access_key=self.aws_secret)

        iam = boto3.client('iam',aws_access_key_id=self.aws_key,
                     aws_secret_access_key=self.aws_secret,
                     region_name='us-west-2'
                  )


        dwhRole = iam.create_role(
            Path='/',
            RoleName=self.role_arn,
            Description="Permitir que os clusters utilizem outros serviços da Amazon",
            AssumeRolePolicyDocument=json.dumps(
                {"Statement": [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
                    )

        iam.attach_role_policy(RoleName=self.role_arn, 
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

        self.iam_role = iam.get_role(RoleName=self.role_arn)['Role']['Arn']

        try:
            response = redshift.create_cluster(
                ClusterType=self.dwh_cluster_type,
                NodeType=self.dwh_node_type,
                NumberOfNodes=int(self.dwh_num_nodes),
                DBName=self.db_name,
                ClusterIdentifier='redshift-2',
                MasterUsername=self.user,
                MasterUserPassword=self.password,
                IamRoles=[self.iam_role])
        except Exception as e:
            print(e)

        while True:
            response = redshift.describe_clusters(ClusterIdentifier='redshift-2')
            print('Aguarde um momento, o cluster está sendo criado! Isso pode levar alguns minutos.')
            current_time = datetime.datetime.now()
            sleep(60)

            if response is not None and response['Clusters'] is not None:
                if response['Clusters'][0]['ClusterStatus'] == 'creating':
                    if (datetime.datetime.now() - current_time).seconds > 120:  
                        print("Aguarde mais um pouco, o cluster ainda está sendo criado")
                elif response['Clusters'][0]['ClusterStatus'] == 'available':
                    self.end_point = response['Clusters'][0]['Endpoint']['Address']
                    break
            else:
                pass




        print("Seu cluster foi criado!!")
        self.__creating_cfg_file__()

    def __creating_cfg_file__(self):
        
        
        file_content = f"""[CLUSTER]
                           HOST={self.end_point}
                           DB_NAME={self.db_name}
         DB_USER={self.user}
        DB_PASSWORD={self.password}
        DB_PORT={self.port}

        [IAM_ROLE]
        ARN='{self.iam_role}'

        [S3]
        BUCKET='s3://data-sprints-test/trips'
        """
        
        print("Sua arquivo de configuração será criado utilizando a seguinte configuração")
        print(file_content)


        with open('dwh.cfg', 'w') as f:
            f.write(file_content)





if __name__ == "__main__":

    config = ConfiguringCloud()
    config.filling_parameters()
    parametros_corretos = "n"
    
    while parametros_corretos != "y":
        config.printing_parameters()
        print()
        parametros_corretos = input('Esses são os valores corretos? (y/n): ')

        if parametros_corretos == 'n':
            config.filling_parameters()

    config.creating_cluster()
    

    
