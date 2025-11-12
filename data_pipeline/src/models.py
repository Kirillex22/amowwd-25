from pydantic import BaseModel, Field
from datetime import date
from typing import Optional


class Record(BaseModel):
    id: str = Field(..., description="uuid идентификатор")
    name: Optional[str] = Field(..., description="имя — может быть пустым")
    age: Optional[int] = Field(..., description="возраст — может быть некорректным с вероятностью 0.5")
    category: Optional[str] = Field(..., description="категориальный признак")
    value: Optional[float] = Field(..., description="количественный признак")
    country: Optional[str] = Field(..., description="страна - может быть None или некорректной с вероятностью 0.5")
    city: Optional[str] = Field(..., description="город - может быть None или некорректен с вероятностью 0.5")
    signup_date: Optional[date] = Field(..., description="дата регистрации - может быть None или в некорректном формате с вероятностью 0.5")
    email: Optional[str] = Field(..., description="электронная почта - может быть None или некорректной с вероятностью 0.5")
