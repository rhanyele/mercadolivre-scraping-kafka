# config/kafka_config.py

KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_OFFER_TOPIC = 'mercadolivre_offers'
KAFKA_DISCOUNT_TOPIC = 'mercadolivre_offers_discounts'
KAFKA_OFFER_GROUP = 'offer_discount_group'
KAFKA_DISCOUNT_GROUP = 'discount_consumer_group'
OFFER_URL = 'https://www.mercadolivre.com.br/ofertas'