from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente a partir do arquivo .env
load_dotenv()

# Credenciais do banco de dados
DB_HOST = os.environ['DB_HOST']  # Host onde o banco de dados está rodando
DB_PORT = os.environ['DB_PORT']  # Porta padrão do PostgreSQL
DB_NAME = os.environ['DB_NAME']  # Nome do banco de dados
DB_USER = os.environ['DB_USER']  # Nome do usuário
DB_PASS = os.environ['DB_PASS']  # Senha do usuário

# Configuração do SQLAlchemy
Base = declarative_base()

class Offer(Base):
    """
    Classe que define o modelo da tabela de ofertas.
    """
    __tablename__ = 'offers'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    old_price = Column(Float)
    new_price = Column(Float)
    discount = Column(Float)
    scrape_datetime = Column(DateTime, default=datetime.now)

def create_db_session():
    """
    Cria uma sessão do SQLAlchemy e conecta ao banco de dados.

    Returns:
        session (Session): Uma sessão do SQLAlchemy.
    """
    # Cria a URL de conexão com o banco de dados
    db_url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    # Cria o engine e conecta ao banco de dados
    engine = create_engine(db_url)

    # Cria as tabelas no banco de dados, se não existirem
    Base.metadata.create_all(engine)

    # Cria uma fábrica de sessões
    Session = sessionmaker(bind=engine)

    # Retorna uma sessão do SQLAlchemy
    return Session()

def create_offer(session, title, old_price, new_price, discount):
    """
    Cria uma nova oferta no banco de dados.

    Args:
        session (Session): Sessão do SQLAlchemy.
        title (str): Título da oferta.
        old_price (float): Preço antigo da oferta.
        new_price (float): Preço novo da oferta.
        discount (float): Desconto da oferta.

    Returns:
        offer (Offer): A oferta criada.
    """
    offer = Offer(title=title, old_price=old_price, new_price=new_price, discount=discount)
    session.add(offer)
    session.commit()
    session.refresh(offer)  # Atualiza a instância para garantir que está associada à sessão
    return offer

def get_all_offers(session):
    """
    Obtém todas as ofertas do banco de dados.

    Args:
        session (Session): Sessão do SQLAlchemy.

    Returns:
        offers (list): Uma lista de todas as ofertas.
    """
    offers = session.query(Offer).all()
    return offers

def delete_offer(session, offer_id):
    """
    Deleta uma oferta do banco de dados.

    Args:
        session (Session): Sessão do SQLAlchemy.
        offer_id (int): ID da oferta a ser deletada.

    Returns:
        bool: True se a oferta foi deletada com sucesso, False caso contrário.
    """
    offer = session.query(Offer).filter_by(id=offer_id).first()
    if offer:
        session.delete(offer)
        session.commit()
        return True
    return False
