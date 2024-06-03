# Projeto Mercado Livre Scraping Kafka

O projeto realiza web-scraping de ofertas do site Mercado Livre, valida e processa essas ofertas, e integra com o banco de dados PostgreSQL, utilizando Kafka para processamento de mensagens.

No projeto foram criados dois tópicos Kafka: um para coletar todas as ofertas e outro para validar os descontos maiores que 50% e fazer a inserção desses produtos com maior desconto no banco de dados.

![mercadolivre-scraping-kafka](https://github.com/rhanyele/mercadolivre-scraping-kafka/assets/10997593/f445aae8-acc1-4f34-92d1-6654398695a7)


### Site de ofertas
- [Mercado Livre ofertas](https://www.mercadolivre.com.br/ofertas)

## Estrutura do projeto
```bash
- src
  - config
    - kafka_config.py
  - database
    - .env
    - connection.py
  - model
    - modelOffers.py
  - discount_consumer.py
  - mercadolivre_scraping.py
  - offer_consumer.py
  - offer_producer.py
- docker-compose.yml
```

## Funcionalidades
- **Scraping de Ofertas:** Extrai dados de ofertas, incluindo títulos, preços antigos e novos, e data/hora de coleta.
- **Validação de Ofertas:** Valida os dados extraídos utilizando modelos Pydantic.
- **Processamento de Ofertas com Desconto:** Calcula e valida descontos, enviando ofertas com mais de 50% de desconto para um tópico Kafka específico.
- **Integração com Banco de Dados:** Armazena e gerencia ofertas em um banco de dados PostgreSQL.


## Requisitos
- Python
- Poetry
- Apache Kafka
- PostgreSQL
- Docker
- Docker Compose

## Instalação
1. Clone este repositório:

   ```bash
   git clone https://github.com/rhanyele/mercadolivre-scraping-kafka.git
   ```

2. Acesse o diretório do projeto:

   ```bash
   cd mercadolivre-scraping-kafka
   ```

3. Instale as dependências usando Poetry:

   ```bash
   poetry install
   ```

4. Configure as variáveis de ambiente no arquivo `database/.env` com as informações do seu banco de dados PostgreSQL:
   ```
   DB_HOST=seu_host
   DB_PORT=sua_porta
   DB_NAME=seu_banco_de_dados
   DB_USER=seu_usuario
   DB_PASS=sua_senha
   ```

5. Execute o docker compose para criar os containers do Apache Kafka:

   ```bash
   docker compose up
   ```

## Uso
1. Faça o Web-Scraping das ofertas:
   ```bash
   poetry run python .\src\mercadolivre_scraping.py
   ```
   
2. Execute o consumer Kafka que valida os descontos e envia para o tópico de desconto:
   ```bash
   poetry run python .\src\offer_consumer.py 
   ```
   
3. Execute o consumer Kafka que salva ofertas com mais de 50% de desconto no PostgreSQL:
   ```bash
   poetry run python .\src\discount_consumer.py
   ```

## Contribuição
Sinta-se à vontade para contribuir com novos recursos, correções de bugs ou melhorias de desempenho. Basta abrir uma issue ou enviar um pull request!

## Autor
[Rhanyele Teixeira Nunes Marinho](https://github.com/rhanyele)

## Licença
Este projeto está licenciado sob a [MIT License](LICENSE).
