import pandas as pd
import re
from datetime import datetime
import os
import psycopg2
from glob import iglob
from sql_queries import etl_queries


def main():
    import configparser
    conf = configparser.ConfigParser()
    conf.read('dwh.cfg')
    

    # criando a conexão
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*conf['CLUSTER'].values()))
    cur = conn.cursor()

    # tabela vendors
    df = pd.read_csv('vendors.csv', header=None)
    for index, row in df.iterrows():
        print("Tentando inserir os dados referentes as empresas de taxi")
        try:
            cur.execute(etl_queries['insert_vendors'], list(row))
            conn.commit()
        except psycopg2.Error as e:
            print("Não foi possível realizar a inserção dos dados na tabela vendors.")
            print(e)

    t1 = datetime.now()
    # tabela trips temporária
    cur.execute(etl_queries["copy_trips"].format(conf["S3"]["BUCKET"], 
                                                 conf["IAM_ROLE"]["ARN"]))
    conn.commit()
    t2 = datetime.now()
    print(f"A inserção dos dados na tabela staging_trips foi realizada com sucesso!")
    print(f"A inserção levou {t2-t1}")

    t1 = datetime.now()
    print("Iniciando a transformação dos dados")
    try:
        cur.execute(etl_queries["etl_trips"])
        conn.commit()
        print("A transformação dos dados foi um sucesso!")
        t2 = datetime.now()
        print(f"A inserção levou {t2-t1}")
    except psycopg2.Error as e:
        print("Não foi possivel transformar os valores")
        print(e)

                                                    
    conn.close()










if __name__ == "__main__":
    main()