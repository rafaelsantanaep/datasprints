import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Essa função irá se conectar ao cluster e irá deletar todas as tabelas existentes
    dentro dele.

    Os queries utilizados para deletar as tabelas estão disponíveis no arquivo sql_queries.py.

    """"    
    for query in drop_table_queries:
        try:
            print(f"{query}")
            cur.execute(query)
            conn.commit()
            print("Tabela deletada com sucesso")
        except psycopg2.Error as e:
            print(e)
            print("Não foi possível deletar a tabela")
        
def create_tables(cur, conn):
    """
    Essa função irá realizar a criação das tabelas no cluster do Amazon Redshift.
    """
    for query in create_table_queries:
        try:
            print(f"{query}")
            cur.execute(query)
            conn.commit()
            print("Tabela criada com sucesso")
        except psycopg2.Error as e:
            print(e)
            print("Não foi possível criar a tablea")

def main():
    """
    A função main será responsável pelo fornecimento dos insumos necessários para a execução
    das outras funções presentes nesse arquivo. Ela é necessária porque antes de deletar as
    tabelas existentes no cluster / recriar as novas tabelas, é necessário criar uma conexão
    com o cluster da Amazon Redshift.

    Para tanto, foi criado um arquivo de configuração que se encontra nessa pasta.

    As informações nesse arquivo serão obtidas através da biblioteca configparser.

    
    """
    # lendo o arquivo com as configurações
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # conexão com a database no cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # deletando e criando as novas tabelas
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # fechando a conexão com o cluster, para evitar conflitos nas próximas iterações.
    conn.close()


if __name__ == "__main__":
    main()