from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from app.services.auth.utils import decode_access_token
from app.db import db

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    username = payload["sub"]
    user = await db.get_user_by_username(username)
    if not user or not user["is_active"]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")

    # Remove sensitive info before returning
    user.pop("hashed_password", None)
    return user
