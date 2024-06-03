import logging
from pydantic import BaseModel, ValidationError, validator
from datetime import datetime

class Offer(BaseModel):
    """
    Modelo Pydantic para representar uma oferta.
    """
    title: str
    old_price: float
    new_price: float
    scrape_datetime: datetime

class OfferDiscount(Offer):
    """
    Modelo Pydantic para representar uma oferta com desconto,
    estendendo o modelo Offer.
    """
    discount: float

    @validator('discount')
    def validate_discount(cls, value):
        """
        Valida que o desconto é maior que 50%.

        Args:
            value (float): Valor do desconto.

        Returns:
            float: Valor do desconto se for válido.

        Raises:
            ValueError: Se o valor do desconto for menor ou igual a 50.
        """
        if value < 50:
            raise ValueError('O desconto precisa ser maior que 50')
        return value

def validate_offer(data):
    """
    Valida os dados da oferta usando o modelo Offer.

    Args:
        data (dict): Dados da oferta a serem validados.

    Returns:
        bool: True se os dados forem válidos, False caso contrário.
    """
    try:
        Offer(**data)
        logging.info("Validação da oferta bem-sucedida.")
        return True
    except ValidationError:
        logging.error("Erro na validação da oferta:", exc_info=True)
        return False

def validate_offer_discount(data):
    """
    Valida os dados da oferta com desconto usando o modelo OfferDiscount.

    Args:
        data (dict): Dados da oferta a serem validados.

    Returns:
        bool: True se os dados forem válidos, False caso contrário.
    """
    try:
        OfferDiscount(**data)
        logging.info("Validação da oferta com desconto bem-sucedida.")
        return True
    except ValidationError:
        logging.error("Erro na validação da oferta com desconto:", exc_info=True)
        return False
