from pydantic import BaseModel, EmailStr
from datetime import datetime

class User(BaseModel):
    id: int
    username: str
    email: EmailStr
    is_active: bool
    created_at: datetime

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str
