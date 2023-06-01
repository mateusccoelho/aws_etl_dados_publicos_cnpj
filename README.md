# ETL com AWS Step Functions - Dados Públicos CNPJ

- [ETL com AWS Step Functions - Dados Públicos CNPJ](#etl-com-aws-step-functions---dados-públicos-cnpj)
  - [Pré-requisitos](#pré-requisitos)
  - [Serviços de dados e analytics na AWS](#serviços-de-dados-e-analytics-na-aws)
  - [Desenho da solução](#desenho-da-solução)
  - [Implementação](#implementação)
    - [IAM Roles](#iam-roles)
    - [S3](#s3)
    - [AWS Glue](#aws-glue)
    - [Lambdas](#lambdas)
      - [check\_update](#check_update)
      - [download\_test](#download_test)
      - [fetch\_data](#fetch_data)
    - [Máquina de estados](#máquina-de-estados)
    - [EventBridge Scheduler](#eventbridge-scheduler)
  - [Próximos passos](#próximos-passos)
  - [Referências](#referências)

Este projeto mostra como utilizar os serviços da [Amazon Web Services (AWS)](https://aws.amazon.com/pt/?nc2=h_lg) para construir um *pipeline* de extração de dados. O objetivo é capturar os [dados do Cadastro Nacional de Pessoas Jurídicas (CNPJ)](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj) disponibilizado pela Receita Federal (RF) em [seu site](https://dadosabertos.rfb.gov.br/CNPJ/). Mais especificamente, extrairemos as seguintes tabelas:

- Empresas
- Sócios
- Estabelecimentos
- Simples
- Municípios
- CNAEs
- Naturezas jurídicas
- Motivos
- Países
- Qualificações

Para isso utilizaremos os seguintes serviços: 

- AWS Lambda: executa funções Python.
- AWS Step Functions: uma máquina de estados que orquestra os outros serviços AWS utilizados.
- Amazon S3: object storage para guardar os arquivos de dados.
- AWS Glue Data Catalog: repositório de metadados das tabelas.
- Amazon EventBridge Scheduler: ferramenta para agendar a execução da máquina de estados.

Uma característica em comum é que são serverless e totalmente gerenciados. Ou seja, só é necessário se preocupar com o código, e não com a infraestrutura.

## Pré-requisitos

- Ter uma conta AWS. Todos os serviços deste tutorial se enquadram no [nível gratuito](https://aws.amazon.com/pt/free/?nc2=h_ql_pr_ft&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all) para contas novas. Porém, se sua conta já for antiga, a boa notícia é que precisei gastar menos de US$ 0,20 para fazer os testes. O resposável pelo maior custo foi o Crawler do Data Catalog.
- Conhecimentos básicos de AWS e do console.
- Ter Python 3.9 instalado na sua máquina.
- Ter conhecimentos básicos de engenharia de dados.

## Serviços de dados e analytics na AWS

Antes de entrar em detalhes de cada serviço, vale a pena explicar como funciona a integração entre os serviços de dados e analytics da AWS.

![](references/aws_data_services_integration.png)

Na AWS cada serviço cuida de uma parte da implantação de um *datalake*. Assim, há uma divisão clara entre os serviços que armazenam, catalogam e consumem dados.

Na base do diagrama temos os serviços de armazenamento, como Amazon S3 e Amazon RDS. É neles onde os dados efetivamente moram. Repare que cada serviço tem suas especificidades, podendo guardar ou não metadados de dados estruturados, semi-estruturados ou não estruturados.

Em seguida, temos o AWS Glue Data Catalog, cujo objetivo é catalogar os metadados de várias fontes de informação em um local único e de forma padronizada. Para isso utilizamos Crawlers, que examinam as fontes de dados e catalogam tabelas, particionamentos, tipos de dados, formatos de arquivo, etc. 

Por último existem os serviços de consumo, como EMR (cluster hadoop), Redshift e Athena. 

Neste projeto trabalharemos apenas nas duas primeiras camadas. Primeiro construiremos as ferramentas para guardar os dados em um bucket S3. Depois utilizaremos um crawler para catalogar os metadados da tabela Empresas em um database do Data Catalog.

## Desenho da solução

![](references/stepfunctions_graph.png)

A solução é baseada em uma máquina de estados para orquestrar as chamadas das lambdas e da API do AWS Glue. Ela será executada semanalmente com o auxílio do Amazon EventBridge. Em linhas gerais, seguiremos o seguinte fluxo:

1. Passamos na entrada da máquina de estados um JSON com a lista de tabelas que queremos extrair e o nome do bucket onde elas serão armazenadas. Por exemplo:

```json
{
    "Tables": ["empresas", "simples", "socios"],
    "BucketName": "projeto-cnpj"
}
```

Note que o pipeline suporta 10 tabelas, mas podemos escolher especificamente quais queremos atualizar.

2. Fazemos uma chamada à API do Glue para obter a lista de tabelas que temos atualmente no database do projeto no Data Catalog. A saída dessa etapa é a entrada juntada com o resultado da chamada. Na primeira execução do ETL não existirão tabelas, então o resultado será, por exemplo:

```json
{
  "Tables": ["empresas", "cnaes", "municipios"],
  "BucketName": "projeto-cnpj",
  "DBOutput": {
    "TableList": []
  }
}
```

Quando rodarmos essa parte com algumas tabelas já criadas, a lista da chave "TableList" será populada com um dicionário para cada tabela. ELes conterão metadados como data de criação, nome da tabela, colunas de particionamento, etc. O JSON abaixo mostra como seria um pequeno trecho dos metadados:

```json
{
    "CatalogId": "numero_da_conta",
    "CreateTime": "2023-04-23T22:20:15Z",
    "CreatedBy": "arn:aws:sts::numero_da_conta:assumed-role/CNPJCrawlerRole/AWS-Crawler",
    "DatabaseName": "cnpj",
    "IsRegisteredWithLakeFormation": false,
    "LastAccessTime": "2023-04-23T22:20:15Z",
    "Name": "empresas",
    "Owner": "owner",
    "Parameters": {
        "sizeKey": "1608647538",
        "objectCount": "10",
        "UPDATED_BY_CRAWLER": "EmpresasCrawler",
        "CrawlerSchemaSerializerVersion": "1.0",
        "recordCount": "53293844",
        "averageRecordSize": "47",
        "CrawlerSchemaDeserializerVersion": "1.0",
        "compressionType": "none",
        "classification": "parquet",
        "typeOfData": "file"
   },
   "PartitionKeys": [
    {
        "Name": "ref_date",
        "Type": "string"
    }
   ],
...
```

No caso deste projeto essas informações não são importantes, pois o objetivo da chamada da API é simplesmente saber quais tabelas já existem e quais precisam ser criadas.

3. Chamamos uma Lambda que irá acessar o site da Receita Federal e fará um *web scrapping* a fim de obter os links dos arquivos de dados e as datas de atualização. A saída dessa etapa terá o seguinte formato:

<details>
    <summary>JSON</summary>
  
    ```json
    {
        "Tables": [
            {
                "name": "empresas",
                "exists": true,
                "files": [
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas1.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas2.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas3.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas4.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas5.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas6.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas7.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas8.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    },
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas9.zip",
                        "table_name": "empresas",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    }
                ],
                "ref_date": 20230516
            },
            {
                "name": "cnaes",
                "exists": false,
                "files": [
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Cnaes.zip",
                        "table_name": "cnaes",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    }
                ],
                "ref_date": 20230516
            },
            {
                "name": "municipios",
                "exists": false,
                "files": [
                    {
                        "url": "https://dadosabertos.rfb.gov.br/CNPJ/Municipios.zip",
                        "table_name": "municipios",
                        "bucket_name": "project-cnpj",
                        "date": 20230516
                    }
                ],
                "ref_date": 20230516
            }
        ]
    }
    ```

</details>

Dentro do dicionário "Tables" teremos um dicionário para cada tabela que queremos processar. Cada dicionário conterá a data da última atualização da tabela segundo o site, uma flag indicando se a tabela já existe na AWS e uma lista de informações sobre cada arquivo que será baixado. Repare que os dicionários sobre os arquivos são meio repetitivos, porém essa estrutura é necessária para o funcionamento da função que baixa cada arquivo individualmente.

4. Iniciamos um loop em que cada tabela será analisada paralelamente e de forma independente. Ou seja, todas os passos da máquina de estados dentro deste loop serão executados uma vez para cada tabela. A entrada desta etapa também será repartida de forma que cada "ramo" do loop recebe um dos dicionários contidos na lista `$.Tables` da etapa anterior. Por exemplo, o ramo que seguirá com a tabela Municípios terá o seguinte JSON:

```json
{
    "name": "municipios",
    "exists": false,
    "files": [
        {
            "url": "https://dadosabertos.rfb.gov.br/CNPJ/Municipios.zip",
            "table_name": "municipios",
            "bucket_name": "project-cnpj",
            "date": 20230516
        }
    ],
    "ref_date": 20230516
}
```

5. Essa etapa controla o fluxo da máquina criando uma estrutura condicional. O elemento `$.exists` da entrada será avaliado. Se a tabela não existir, o fluxo irá diretamente para o passo 9. Se existir, o fluxo continua no passo 6 pois um teste adicional precisa ser feito.

6. Essa etapa fará uma chamada de API do Glue para coletar os metadados das partições da tabela. O objetivo aqui é criar uma lista com os valores das partições da tabela. O resultado, portanto, terá o seguinte formato:

<details>
    <summary>JSON</summary>

    ```json
    {
        "name": "empresas",
        "exists": true,
        "files": [
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas1.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas2.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas3.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas4.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas5.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas6.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas7.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas8.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            },
            {
                "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas9.zip",
                "table_name": "empresas",
                "bucket_name": "projeto-cnpj",
                "date": 20230516
            }
        ],
        "ref_date": 20230516,
        "GetPartitionsOutput": {
            "partitionValues": [
                [
                    "20230516"
                ],
                [
                    "20230411"
                ]
            ]
        }
    }
    ```
</details>


Repare que no final do JSON um novo dicionário contendo as partições foi agregado.

7. A entrada será processada por uma função Python que simplesmente fará um teste se a data de atualização coletada no site da RF é maior que a partição mais recente coletada no passo anterior. Esse teste é útil porque não precisamos baixar os arquivos da tabela se ela não sofreu atualização.

## Implementação

Recomendo usar a região N. Virginia (us-east-1) pois é a mais barata e a latência neste projeto é irrelevante.

### IAM Roles

Ao longo do tutorial precisaremos definir algumas *roles* para dar a cada recurso AWS os acessos necessários para funcionarem. Tentaremos seguir o princípio de acesso mínimo, em que não concedemos nada além do necessário para cada *role*. Não se esqueça de substituir o número da sua conta, região e outras informações nas *policies* mostradas adiante.

### S3

Será necessário um bucket S3 cuja estrutura será dada pelo diagrama abaixo.

```
.
└── nome_do_seu_bucket/
    └── cnpj_db/
        └── empresas/
            ├── ref_date=20230416/
            │   ├── file1
            │   ├── file2
            │   ├── ...
            │   └── fileN
            └── ref_date=20230516/
                ├── file1
                ├── file2
                ├── ...
                └── fileN
```

Note que usamos "pastas" para representar e separar databases, tabelas e partições. A princípio nenhuma pasta precisa ser criada pois isso será feito automaticamente pela Lambda `fetch_data`.

Lembre que no S3 não existem pastas. Conforme explicado [nessa thread](https://stackoverflow.com/questions/1939743/amazon-s3-boto-how-to-create-a-folder):

> There is no concept of folders or directories in S3. You can create file names like "abc/xys/uvw/123.jpg", which many S3 access tools like S3Fox show like a directory structure, but it's actually just a single file in a bucket.

### AWS Glue

No Glue será necessário criar um database e um Crawler. A criação do database é super simples pelo console, bastando apenas passar um nome. Na criação do Crawler preste atenção nas seguinte configurações:

- Configure uma fonte de dados originada do bucket S3 que criamos anteriormente. A localização sobre a qual o crawler atuará será `s3://nome_do_seu_bucket/cnpj_db`. Ou seja, ele varrerá os arquivos de todas as tabelas que criaremos.
- Use a IAM Role `CNPJCrawlerRole` que definiremos abaixo.
- Nas configurações de saída use o database criado anteriomente.
- No agendamento do Crawler selecionar sob-demanda.

Segundo a documentação, é recomendado que a *role* do Crawler `CNPJCrawlerRole` tenha 

1. a *policy* gerenciada pela AWS `AWSGlueServiceRole`;
2. uma *policy* com os acessos específicos da fonte de dados que ele irá acessar.

A segunda *policy* deve ter o seguinte conteúdo:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::nome_do_seu_bucket/cnpj_db*"
            ]
        }
    ]
}
```

### Lambdas

O AWS Lambda é um serviço que tenta abstrair ao máximo a execução de uma função. [Esta documentação](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html) explica de forma resumida e completa o que é ele faz, como ele pode ser disparado, quais as suas capacidades, etc.

Neste projeto usaremos três funções que rodam em ambiente Python 3.9: `check_update`, `fetch_data` e `download_test`. O código está organizado na pasta `lambdas`. A pasta de cada lambda contém:

1. O código que de fato será executado.
2. O arquivo `requirements.txt`, que lista as dependências da função.
3. Um bash script para gerar o *deployment package*.

Cada bash script deve ser executado a partir da pasta onde está contido. Eles fazem o seguinte:

1. Cria um virtual env.
2. Instala as dependências.
3. Cria o arquivo .zip com o código da função e das dependências.

Os arquivos zip devem ser subidos no console da AWS.

Sobre as *roles*, a tabela abaixo mostra as roles que cada função deve assumir.

| Nome          | Role             |
|---------------|------------------|
| check_update  | SimpleLambdaRole |
| fetch_data    | LambdaRoleWithS3 |
| download_test | SimpleLambdaRole |

A *role* `SimpleLambdaRole` é similar à criada automaticamente pelo console e tem o seguinte template: 

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:regiao:numero_da_sua_conta:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:regiao:numero_da_sua_conta:log-group:*"
            ]
        }
    ]
}
```

A *role* `LambdaRoleWithS3` usará a *policy* da `SimpleLambdaRole` e mais a *policy* definida abaixo:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::nome_do_seu_bucket/*"
        }
    ]
}
```

#### check_update

Esta função não tem input. Ela faz um web scraping simples para extrair a data de alteração dos arquivos da tabela Empresas no [site da Receita Federal](https://dadosabertos.rfb.gov.br/CNPJ/).

Analisando o código-fonte da página, percebe-se que é simplesmente uma tabela HTML. Portanto, utilizamos o pacote `beautifulsoup` para tratar a tabela, encontrar a linha correspondente ao arquivo `Empresas0.zip` e extrair a sua data de alteração. É esta data que será passada adiante no pipeline. O retorno da função será um dicionário neste formato:

```json
{
  "statusCode": 200,
  "body": {"ref_date": 20230416}
}
```

Conforme descrito acima, o *deployment package* desta função deve ser construído e carregado na AWS. Na criação da lambda recomendo alterar as seguintes propriedades:

- Timeout igual a 15s. As vezes a requisição ao site da RF demora.
- Atualizar o nome do arquivo no Handler.

![](references/handler_conf.png)

#### download_test

Esta função recebe um JSON semelhante ao exposto abaixo, o qual é gerado durante a execução da máquina de estados.

```json
{
  "LambdaResult": {
    "ref_date": 20230315
  },
  "Tables": {
    "TableList": [
      
    ]
  },
  "partitionValues": {
    "partitionValues": [
      [
        "20230315"
      ]
    ]
  }
}
```

Ela faz uma comparação de datas para verificar se devemos atualizar a tabela com os dados disponibilizados no site da RF. Como `$.partitionValues.partitionValues` é um array, primeiro eu encontro o valor máximo da lista e só depois faço a comparação com o `$.LambdaResult.ref_date`.

O retorno terá o seguinte formato:

```json
{
  "statusCode": 200,
  "body": false
}
```

Como esta função não tem dependências, não é necessário criar o *deployment package*. Após criar a lambda, o código pode ser simplesmente colado no editor de texto do console.

#### fetch_data

Esta função faz o download, tratamento e armazenamento dos arquivos da tabela Empresas. Cada chamada trata um dos arquivos individualmente e deve receber um *payload* semelhante ao abaixo:

```json
{
  "url": "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip",
  "table_name": "empresas",
  "date": "20230416",
  "bucket_name": "projeto-cnpj"
}
```

Em linhas gerais, a função faz o seguinte:

1. Usa o pacote `urllib` para baixar o arquivo cujo link foi passado no payload. Este arquivo é salvo no disco efêmero do lambda, localizado na pasta `/tmp/`. 
2. O arquivo .zip é extraído e o CSV é armazenado em `/tmp/`.
3. O CSV é convertido em Parquet usando o pacote `pyarrow`.
4. O Parquet é enviado ao bucket S3 dentro da estrutura de "pastas" pré-definida para receber os arquivos das tabelas.

Conforme descrito acima, o *deployment package* desta função deve ser construído e subido na AWS. Porém, no caso desta função, como o arquivo é maior que 50 Mb, não é possível subí-lo diretamente no console. Devemos carregá-lo no bucket criado anteriormente. 

Devido ao tamanho dos arquivos manipulados, na criação da lambda é necessário alterar as seguintes propriedades:

- Atualizar o nome do arquivo no Handler.
- Limite de memória RAM: 3008 MB.
- Limite de armazenamento efêmero: 2048 MB.
- Timeout: 8 min.

### Máquina de estados

AWS Step Funcitions é um serviço da AWS para orquestrar chamadas de API em formato de uma máquina de estados. Podemos representar tal máquina como um grafo, conforme mostrado na seção desenho da solução ![desenho da solução](#desenho-da-solução). Os nós representam estados (*states*), que podem ser chamadas de API ou controladores do fluxo de execução, como o *Choice* ou o *Map*. As arestas indicam como será o fluxo de execução da máquina.

Cada estado recebe e retorna um documento JSON, o qual pode ser tratado de algumas formas. A figura abaixo (retirada da documentação oficial) mostra como flui a informação dentro de um estado.

![](https://docs.aws.amazon.com/images/step-functions/latest/dg/images/input-output-processing.png)

Cada etapa intermediária na caixa verde representa um filtro ou tratamento que pode ser feito no *input* e *output* do estado. No caso deste projeto, o *input* de alguns estados dependem das informações extraídas nos estados anteriores. Por exemplo, os estados *choice* utilizam o *input* para definir regras que controlam o fluxo de exeução. Assim, essa funcionalidade é essencial para configurarmos como os JSON serão passados adiante.

![](references/choice_state.png)

Usaremos 4 tipos de estados:

- Invocação de lambdas.
- Chamadas de API do AWS Glue para obter listas de tabelas e partições e disparar um Crawler.
- *Choice state* para direcionar o fluxo da máquina de estados quando a tabela não existe ou a partição nova ainda não foi ingerida no S3.
- *Map state* para rodar em paralelo a extração dos 10 arquivos da tabela Empresas.

A máquina de estados pode ser configurada de maneira visual pelo Workflow Studio ou em formato de texto usando a linguagem Amazon States Language (ASL). Ela é baseada em JSON e é fácil de entender. Entretanto, neste projeto eu usei o Studio e extraí o código ASL resultante, o qual está na pasta `state_machine`. Isso deixou a minha vida muito fácil porque usar o Studio economiza muitas consultas à documentação oficial. 

Para usar o código aqui disponibilizado será necessário substituir os valores cercados por `||` pelo ARN ou nome do recurso correspondente (removendo os `||` também). Na criação da máquina no console basta selecionar a opção de criar a partir de código. Além disso, selecione o tipo *standard*.

Precisaremos de uma *role* personalizada chamada `CNPJStateMachineRole` que tem a seguinte *policy*:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": [
                "arn:aws:lambda:regiao:numero_da_sua_conta:function:fetch_data",
                "arn:aws:lambda:regiao:numero_da_sua_conta:function:check_update",
                "arn:aws:lambda:regiao:numero_da_sua_conta:function:download_test"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTables",
                "glue:GetPartitions",
                "glue:StartCrawler",
                "glue:GetTable"
            ],
            "Resource": [
                "arn:aws:glue:regiao:numero_da_sua_conta:database/nome_do_db",
                "arn:aws:glue:*:numero_da_sua_conta:catalog",
                "arn:aws:glue:regiao:numero_da_sua_conta:table/nome_do_db/*",
                "arn:aws:glue:regiao:numero_da_sua_conta:crawler/nome_do_crawler"
            ]
        }
    ]
}
```

Após a configuração você já pode executar o ETL! Note que não precisamos de um JSON de input na máquina pois toda a informação necessária será capturada em tempo de execução ou está *hard-coded* no arquivo ASL. Se tudo foi configurado corretamente até aqui, na primeira execução a tabela será criada e o fluxo será o da imagem abaixo:

![](references/1exec.png)

Nas próximas execuções, se não houver novos dados para ingerir, o fluxo será o da imagem abaixo:

![](references/2exec.png)

### EventBridge Scheduler

A criação do agendamento é bem fácil. Vá até o console e entre em Amazon EventBridge > Scheduler > Schedules > Create Schedule. Alguns pontos para se atentar:

- Selecionar um agendamento recorrente com disparo a cada 7 dias.
- Não é necessário definir uma janela de tempo flexível.
- No target selecione "StartExecution" do serviço AWS Step Functions.
- Selecione a máquina de estados criada anteriormente. Não é necessário passar um input.
- Não é necessária política de *retry*. 
- Deixe que o console crie uma *role* para o scheduler, mas redefina o seu nome para `CNPJSchedulerRole`.

A *role* terá o seguinte formato:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution"
            ],
            "Resource": [
                "arn:aws:states:regiao:numero_da_conta:stateMachine:nome_da_maquina"
            ]
        }
    ]
}
```

## Próximos passos

- Criar um template CloudFormation para instanciar os recursos automaticamente.
- Criar testes unitários.

## Referências

- https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
- https://docs.aws.amazon.com/glue/latest/dg/crawler-prereqs.html
- https://docs.aws.amazon.com/step-functions/latest/dg/concepts-input-output-filtering.html