from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.security import OAuth2PasswordRequestForm
from typing import Optional
from datetime import timedelta

from app.db import db
from app.schemas.user import UserCreate, UserLogin, User
from app.services.auth.utils import hash_password, verify_password, create_access_token
from app.services.auth.dependencies import get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])

ACCESS_TOKEN_EXPIRE_MINUTES = 60  # 1 hour, can be loaded from config

@router.post("/register", response_model=User)
async def register(user: UserCreate):
    print("Registering user:", user)
    # Check if user already exists
    existing_user = await db.get_user_by_username(user.username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")

    hashed_pwd = hash_password(user.password)
    new_user = await db.create_user(username=user.username, email=user.email, hashed_password=hashed_pwd)
    return new_user

@router.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    print("Logging in user:", form_data.username)
    user = await db.get_user_by_username(form_data.username)
    
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token({"sub": user["username"]}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me", response_model=User)
async def read_current_user(current_user=Depends(get_current_user)):
    return current_user

@router.post("/refresh")
async def refresh_token(current_user=Depends(get_current_user)):
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    new_token = create_access_token({"sub": current_user["username"]}, expires_delta=access_token_expires)
    return {"access_token": new_token, "token_type": "bearer"}
