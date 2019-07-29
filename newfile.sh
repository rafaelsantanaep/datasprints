#for i in "2009" "2010" "2011" "2012"; do
    #echo "Baixando as viagens do ano de $i"
#man wget "https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2009-json_corrigido.json" "https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2010-json_corrigido.json" "https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2011-json_corrigido.json" "https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json" 
#done

echo "Agora, seu cluster será configurado!"
key="Key"



function aws_config()
{
    echo "Digite aqui sua $key da AWS"
    read AWS_INFO
    echo "Sua $key está correta?"
    echo $AWS_INFO
    read user_input

    if [[ $user_input = "y" ]]
        then echo "Agora podemos continuar o processo de criação do seu cluster"
    else echo "Tente novamente"
    fi
        
}

user_input=""


# ID
while [[ $user_input != "y" ]]
do 
    aws_config
done


AWS_KEY=$AWS_INFO


key="SECRET"
user_input=""

# SECRET
while [[ $user_input != "y" ]]
do 
    aws_config
done


AWS_SECRET=$AWS_INFO

echo $AWS_KEY $AWS_SECRET

echo "Seu cluster está sendo criado"

echo "Você já baixou os arquivos (y/n)"
read download
if [ $download = "y" ]
    then echo "Esse processo pode levar alguns minutos." 
else 
    echo "Enquanto isso, os dados que serão utilizados serão baixodos e armazenados na pasta data. Em um cenário ideal, os arquivos seriam baixados diretamente pelo cluster através de uma conexão com o bucket, no entanto, como isso não é possível preferi adotar essa estratégia para o download dos arquivos do cluster. Essa é a etapa que leva mais tempo e vai variar de acordo com a conexão do computador. Infelizmente, a conexão ainda é o maior gap existente para ampliação da eficiẽncia do processo de engenharia de dados."                                                 
    echo 
    echo "======================================================================================================================================>"
    years=(2009 2010 2011 2012)
    for element in "${years[@]}"; do
        echo "https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-$element-json_corrigido.json"
    done

fi
#echo "Criando as tabelas"
#python create_tables.py


