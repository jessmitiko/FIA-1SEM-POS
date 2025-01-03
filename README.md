# FIA-1SEM-POS
Projeto da Pós-Graduação Big Data e Eng. de Dados da FIA.

# 📌 Objetivo do Projeto
Construção de um pipeline de dados near realtime com atualização de 2 em 2 minutos contento as posições das linhas de ônibus da cidade de São Paulo, o projeto utiliza a API Olho Vivo disponibilizada gratuitamente pela Prefeitura de São Paulo.  
\+ Como entregável adicional construimos um dashboard simples no Power BI para a apresentação dos dados já devidamente tratados.

## 🛠️ Arquitetura Inicial da Solução
![](/docs/Screenshot_2025-01-03_180617.png)  
 
## 🛠️ Arquitetura Final da Solução  
![](/docs/Screenshot_2025-01-03_182843.png)  
### Mudanças do MVP para a arquitetura final
- O uso do delta foi descontinuado por conta de uma incompatibilidade entre o delta-core [maven lib] e a versão do Spark utilizada na imagem Docker que estávamos utilizando, por conta disso substituímos os formatos das camadas `silver` e `gold` de delta para CSV.
- Descontinuamos o uso do NiFi, é uma ferramenta muito limitada a configuração da extração e transformação de dados via interface, o que dificulta a configuração sistemática via Docker Compose, nesse caso utilizamos o próprio cluster do Spark para executar a extração do dado da API.
- O Hive não foi utilizado pela falta de tempo para estudar como configurar a ferramenta, o uso dele se fazia necessário já que o Power BI precisa de uma SQL engine para acessar de forma mais simples o dado do MinIO, até onde entendi o Hive faz uso do Presto como SQL Engine. Como substituto utilizamos um script em Python dentro de uma Connection criada dentro do próprio arquivo .pbix.

# 🖥️ Como rodar o projeto localmente?
☠️ _Testado somente no Windows, talvez em Linux dê algum pau por causa de estrutura de diretório e quebra de linha de arquivo, vai saber._ ☠️

1. Confirme que você tem o Docker instalado executando `docker --version` no terminal. E veja se o daemon do docker está rodando `docker container ls`.
2. ⚠️ [Crie uma chave de acesso no Site da SPTrans para usar a API Olho Vivo](https://www.sptrans.com.br/desenvolvedores) e use esta chave em todos os jobs de ingestão. ⚠️
    - airflow/jobs/bronze/
        - ingestion-linhas.py
        - ingestion-paradas_by_linhas.py
        - ingestion-posicao_by_linha.py
3. Execute no terminal dentro do diretório raiz do projeto `docker compose up -d`.
4. Acesse o Airflow com o usuário `airflow` e senha `airflow` [padrão na imagem Docker Oficial] e despause as DAGs com o prefixo `ingestion-` e `final-`.
5. [Adicional] Necessário ter o Power BI Desktop instalado para abrir o dashboard.
    - dash/dashboard.pbix

## Acesso aos Containers Docker
- http://localhost:9001/ [MinIO]
- http://localhost:8889/ [Jupyter Notebook]
- http://localhost:8080/ [Airflow]
- http://localhost:38080/ [Spark]

# 🛠️ How-to
- Para criar novos jobs ou DAGs utilize os templates em:
    - airflow/dags/spark-submit-template.py
    - airflow/jobs/spark-job-template.py

# 📖 Links de Referencia
- [API DO OLHO VIVO - Guia de Referência](https://www.sptrans.com.br/desenvolvedores/api-do-olho-vivo-guia-de-referencia/documentacao-api/)
- [Arquitetura medallion](https://www.databricks.com/br/glossary/medallion-architecture)
- [O que é Delta Lake?](https://docs.databricks.com/pt/delta/index.html)
- [Presto SQL Engine](https://prestodb.io/)
