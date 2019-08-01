## Introdução 

Antes de entrar um pouco nos insights que podem ser extraídos através do banco de dados é imporante detalhar um pouco o processo de criação do mesmo. 


1. O primeiro passo foi realizar uma exploração dos dados simples identificar potenciais problemas que poderiam ocorrer durante o processo de inserção dos dados em um banco de dados. Além de ser uma etapa muito importante para definição dos tipos de dados que serão utilizados. 


Ainda nessa etapa, foi realizada uma prototipação das consultas que seriam utilizadas para obter os dados solicitados do banco de dados. Essa etapa foi feita em uma amostra do banco de dados com 100.000 linhas (aleatóriamente selecionadas) para permitir uma comparação entre os resultados obtidos por meio de uma consulta no banco de dados e os resultados obtidos utilizando a biblioteca `pandas`

2. Automação do processo de ETL através da utilização de scripts de python e um script do bash (Shell):
  - create_tables.py
  - etl.py
  - sql_queries.py
  - cloud_config.py
  - automate_elt.sh
  
Para tanto, houve a criação de um script que através da obtenção de dois inputs do usuário: `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` permite automatizar todo o processo de criação de um cluster na Nuvem especificamente para esse projeto.

Após a criação desses cluster, um arquivo de configuração era criado para ser utilizado pelos

  
3. Nessa última etapa, através da utilização do cluster 
    