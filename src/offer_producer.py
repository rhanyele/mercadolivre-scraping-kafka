import logging
from confluent_kafka import Producer
import json
from config.kafka_config import KAFKA_BROKER_URL, KAFKA_OFFER_TOPIC

# Configuração do logger
logging.basicConfig(level=logging.INFO, filename="logs/offer_producer.log", filemode='w', format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")

class KafkaProducer:
    def __init__(self):
        """
        Inicializa o produtor Kafka com as configurações necessárias.
        """
        self.producer = Producer({'bootstrap.servers': KAFKA_BROKER_URL})
        logging.info("Kafka producer criado.")

    def send_to_kafka(self, data):
        """
        Envia uma mensagem para o tópico Kafka especificado.
        
        Args:
            data (dict): Os dados a serem enviados ao tópico Kafka.
        """
        try:
            self.producer.produce(KAFKA_OFFER_TOPIC, key=str(data['title']), value=json.dumps(data))
            self.producer.flush()
            logging.info(f"Mensagem enviada com sucesso para o tópico {KAFKA_OFFER_TOPIC}: {data}")
        except Exception as e:
            logging.error(f"Erro ao enviar dados para o Kafka: {e}")