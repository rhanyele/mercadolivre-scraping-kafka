import logging
from confluent_kafka import Consumer, KafkaError, Producer
import json
from config.kafka_config import KAFKA_BROKER_URL, KAFKA_OFFER_TOPIC, KAFKA_DISCOUNT_TOPIC, KAFKA_OFFER_GROUP

# Configuração do logger
logging.basicConfig(level=logging.INFO, filename="logs/offer_consumer.log", filemode='w', format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")

def create_kafka_consumer():
    """
    Cria e configura um consumidor Kafka para o tópico de ofertas.
    
    Returns:
        Consumer: Um objeto Consumer configurado.
    """
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER_URL,
        'group.id': KAFKA_OFFER_GROUP,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_OFFER_TOPIC])
    logging.info(f"Kafka consumer criado e inscrito no tópico de ofertas:{KAFKA_OFFER_GROUP}.")
    return consumer

def create_kafka_producer():
    """
    Cria e configura um produtor Kafka.
    
    Returns:
        Producer: Um objeto Producer configurado.
    """
    producer = Producer({'bootstrap.servers': KAFKA_BROKER_URL})
    logging.info("Kafka producer criado.")
    return producer

def calculate_discount(offer):
    """
    Calcula o desconto com base nos preços antigo e novo da oferta.
    
    Args:
        offer (dict): Dados da oferta contendo os preços.
    
    Returns:
        float: Percentual de desconto, ou None em caso de erro.
    """
    try:
        old_price = float(offer.get("old_price"))
        new_price = float(offer.get("new_price"))
        discount = round(((old_price - new_price) / old_price) * 100, 2)
        logging.info(f"Desconto da oferta calculado: {discount}%")
        return discount
    except Exception as e:
        logging.error(f"Erro ao calcular desconto: {e}")

def consume_and_process_messages(consumer, producer):
    """
    Consome mensagens do tópico Kafka, calcula o desconto e envia as ofertas com
    desconto maior que 50% para um novo tópico.
    
    Args:
        consumer (Consumer): O consumidor Kafka configurado.
        producer (Producer): O produtor Kafka configurado.
    """
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(f"Erro no consumer: {msg.error()}")
                continue

        try:
            offer = json.loads(msg.value().decode('utf-8'))
            logging.info(f"Recebendo oferta: {offer}")

            discount = calculate_discount(offer)
            if discount is not None and discount >= 50:
                logging.info(f"Oferta com mais de 50% de desconto: {offer}")
                try:
                    offer_with_discount = offer.copy()
                    offer_with_discount['discount'] = discount
                    producer.produce(KAFKA_DISCOUNT_TOPIC, key=str(offer['title']), value=json.dumps(offer_with_discount))
                    producer.flush()
                    logging.info("Sucesso ao enviar a mensagem para o tópico de desconto!")
                except Exception as e:
                    logging.error(f"Erro ao enviar a mensagem para o tópico de desconto: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar mensagem: {e}")
        except Exception as e:
            logging.error(f"Erro ao processar mensagem: {e}")

if __name__ == "__main__":
    try:
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()
        consume_and_process_messages(consumer, producer)
    except Exception as e:
        logging.critical(f"Erro fatal: {e}")
