import logging
from confluent_kafka import Consumer, KafkaError
import json
from config.kafka_config import KAFKA_BROKER_URL, KAFKA_DISCOUNT_TOPIC, KAFKA_DISCOUNT_GROUP
from model.modelOffers import validate_offer_discount
from database.connection import create_db_session, create_offer

# Configuração do logger
logging.basicConfig(level=logging.INFO, filename="logs/discount_consumer.log", filemode='w', format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")

def create_kafka_consumer():
    """
    Cria e configura um consumidor Kafka para o tópico de descontos.
    
    Returns:
        Consumer: Um objeto Consumer configurado.
    """
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER_URL,
        'group.id': KAFKA_DISCOUNT_GROUP,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_DISCOUNT_TOPIC])
    logging.info("Kafka consumer criado e inscrito no tópico de descontos.")
    return consumer

def process_discounted_offer(offer_data):
    """
    Processa a oferta com desconto, validando e armazenando no banco de dados.
    
    Args:
        offer_data (dict): Dados da oferta com desconto.
    """
    try:
        session = create_db_session()
        title = offer_data.get("title")
        old_price = offer_data.get("old_price")
        new_price = offer_data.get("new_price")
        discount = offer_data.get("discount")

        offer = create_offer(session, title, old_price, new_price, discount)
        session.close()
        
        logging.info(f"Oferta recebida: {offer.title}, Desconto: {offer.discount}%")
    except Exception as e:
        logging.error(f"Erro ao processar a oferta: {e}")

def consume_discounted_offers(consumer):
    """
    Consome ofertas com desconto do tópico Kafka, valida e processa as ofertas.
    
    Args:
        consumer (Consumer): O consumidor Kafka configurado.
    """
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(f"Erro no consumidor: {msg.error()}")
                continue

        try:
            offer = json.loads(msg.value().decode('utf-8'))
            if validate_offer_discount(offer):
                logging.info(f"Oferta validada: {offer}")
                process_discounted_offer(offer)
            else:
                logging.warning(f"Oferta inválida: {offer}")
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar mensagem: {e}")
        except Exception as e:
            logging.error(f"Erro ao consumir oferta: {e}")

if __name__ == "__main__":
    try:
        consumer = create_kafka_consumer()
        consume_discounted_offers(consumer)
    except Exception as e:
        logging.critical(f"Erro fatal: {e}")
