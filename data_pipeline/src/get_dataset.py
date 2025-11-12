from typing import List, Optional
from datetime import date
import random
import uuid

import factory
from faker import Faker
from pydantic import BaseModel, Field

from data_pipeline.src.models import Record

fake = Faker()

PROBA = 0.1  # вероятность генерации фиксированного UUID для тестирования дубликатов
static_uuid = str(uuid.uuid4())

def get_static_uuid() -> str:
    print(f"Using static UUID for testing: {static_uuid}")
    return static_uuid

class RecordFactory(factory.Factory):
    """
    Фабрика для генерации записей.
    """
    class Meta:
        model = Record

    id = factory.LazyFunction(lambda: str(uuid.uuid4()) if random.random() > PROBA else get_static_uuid())
    name = factory.LazyFunction(lambda: fake.name() if random.random() > 0.2 else None)
    age = factory.LazyFunction(lambda: random.randint(1, 99) if random.random() > 0.5 else None)
    category = factory.LazyFunction(lambda: random.choice(["A", "B", "C", "D"]))
    value = factory.LazyFunction(lambda: round(random.uniform(0.0, 1000.0), 2))
    country = factory.LazyFunction(lambda: fake.country() if random.random() > 0.5 else fake.word())
    city = factory.LazyFunction(lambda: fake.city() if random.random() > 0.5 else None)
    signup_date = factory.LazyFunction(
        lambda: fake.date_between(start_date="-2y", end_date="today")
        if random.random() > 0.5 else None
    )
    email = factory.LazyFunction(
        lambda: fake.email() if random.random() > 0.5 else "invalid_email@" + fake.word()
    )


def get_dataset(n_rows: int = 200, use_static_uuid=False) -> List[Record]:
    """
    Генерация набора данных через factory_boy.
    """
    if use_static_uuid:
        PROBA = 0
    return [RecordFactory() for _ in range(n_rows)]