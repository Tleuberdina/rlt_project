import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем .env из папки config
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

class Config:
    """Конфигурация приложения"""
    
    # Telegram Bot
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    
    # Database
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'video_stats')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    @classmethod
    def validate(cls):
        """Проверка обязательных переменных"""
        if not cls.TELEGRAM_BOT_TOKEN:
            raise ValueError("TELEGRAM_BOT_TOKEN не установлен в .env файле")
        
        if not cls.DB_PASSWORD:
            print("⚠️  Предупреждение: DB_PASSWORD не установлен")
        
        return True
    
    @classmethod
    def get_db_params(cls):
        """Параметры подключения к БД"""
        return {
            'host': cls.DB_HOST,
            'port': cls.DB_PORT,
            'database': cls.DB_NAME,
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD,
        }
