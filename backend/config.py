import os

class Config:
    DB_HOST = os.getenv("DB_HOST", "db")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "salary_db")
    DB_USER = os.getenv("DB_USER", "salary_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
