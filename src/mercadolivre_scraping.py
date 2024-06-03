import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from model.modelOffers import validate_offer
from offer_producer import KafkaProducer
from config.kafka_config import OFFER_URL

# Configuração do logger
logging.basicConfig(level=logging.INFO, filename="logs/scraping.log", filemode='w', format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")

def get_doc(session, url):
    """
    Obtém e retorna o documento HTML da URL especificada.
    
    Args:
        session (requests.Session): A sessão de requests.
        url (str): A URL da página a ser obtida.
    
    Returns:
        BeautifulSoup: O documento HTML parseado, ou None em caso de erro.
    """
    try:
        response = session.get(url)
        response.raise_for_status()
        doc = BeautifulSoup(response.text, 'html.parser')
        logging.info(f"Documento obtido com sucesso da URL: {url}")
        return doc
    except Exception as e:
        logging.error(f"Falha ao obter o documento da URL {url}: {e}")
        return None

def is_next_page_enabled(doc):
    """
    Verifica se o botão "próxima página" está habilitado no documento HTML.
    
    Args:
        doc (BeautifulSoup): O documento HTML.
    
    Returns:
        bool: True se o botão "próxima página" estiver habilitado, False caso contrário.
    """
    try:
        next_button = doc.find('li', class_='andes-pagination__button--next')
        enabled = next_button and 'andes-pagination__button--disabled' not in next_button.attrs.get('class', [])
        logging.info(f"Próxima página habilitada: {enabled}")
        return enabled
    except Exception as e:
        logging.error(f"Falha ao verificar se a próxima página está habilitada: {e}")
        return False

def get_items_title(doc):
    """
    Obtém os títulos dos itens listados no documento HTML.
    
    Args:
        doc (BeautifulSoup): O documento HTML.
    
    Returns:
        list: Uma lista de títulos dos itens.
    """
    try:
        title_tags = doc.find_all('p', class_='promotion-item__title')
        titles = [tag.text.strip() for tag in title_tags]
        logging.info(f"Títulos dos itens obtidos: {titles}")
        return titles
    except Exception as e:
        logging.error(f"Falha ao obter os títulos dos itens: {e}")
        return []

def get_item_prices(doc, price_type):
    """
    Obtém os preços dos itens listados no documento HTML.
    
    Args:
        doc (BeautifulSoup): O documento HTML.
        price_type (str): O tipo de preço a ser obtido ("new" ou "old").
    
    Returns:
        list: Uma lista de preços dos itens.
    """
    try:
        if price_type == "new":
            price_tags = doc.find_all('div', class_='andes-money-amount-combo__main-container')
        elif price_type == "old":
            price_tags = doc.find_all('s', class_='andes-money-amount-combo__previous-value')
        else:
            return []

        prices = [tag.text.replace('.', '').replace(',', '.').replace('R$', '').strip() for tag in price_tags]
        logging.info(f"Preços ({price_type}) dos itens obtidos: {prices}")
        return prices
    except Exception as e:
        logging.error(f"Falha ao obter os preços dos itens ({price_type}): {e}")
        return []

def scrape_page_offer(doc, producer):
    """
    Realiza o scraping dos dados de uma página e envia as ofertas validadas para o Kafka.
    
    Args:
        doc (BeautifulSoup): O documento HTML.
        producer (KafkaProducer): O produtor Kafka para enviar os dados.
    """
    if doc is None:
        logging.warning("Documento HTML está vazio, abortando scraping.")
        return
    
    titles = get_items_title(doc)
    old_prices = get_item_prices(doc, "old")
    new_prices = get_item_prices(doc, "new")

    for title, old_price, new_price in zip(titles, old_prices, new_prices):
        item = {
            'title': title,
            'old_price': old_price,
            'new_price': new_price,
            'scrape_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        if validate_offer(item):
            logging.info(f"Dados validados: {item}")
            producer.send_to_kafka(item)
        else:
            logging.warning(f"Dados inválidos: {item}")

def scraping_mercadolivre_offers():
    """
    Realiza o scraping das ofertas do Mercado Livre, navegando por múltiplas páginas,
    validando os dados e enviando para o Kafka.
    """
    session = requests.Session()
    producer = KafkaProducer()
    doc = get_doc(session, OFFER_URL)
    
    while doc:
        scrape_page_offer(doc, producer)
        if not is_next_page_enabled(doc):
            break
        
        next_button_link = doc.find('li', class_='andes-pagination__button--next').find('a')
        if next_button_link:
            next_page_url = next_button_link['href']
            doc = get_doc(session, next_page_url)
            time.sleep(2)  # Pausa para evitar sobrecarregar o servidor
        else:
            break

if __name__ == "__main__":
    scraping_mercadolivre_offers()
