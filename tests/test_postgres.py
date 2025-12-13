import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.config import Config
import psycopg2

def test_connection():
    """Тест подключения к PostgreSQL"""
    try:
        # Получаем параметры из единой конфигурации
        params = Config.get_db_params()
        
        conn = psycopg2.connect(**params)
        print("✅ Подключение к PostgreSQL успешно!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        print(f"Версия PostgreSQL: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM videos;")
        print(f"Количество видео: {cursor.fetchone()[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        print("\nПроверьте:")
        print(f"1. Файл config/.env существует и заполнен")
        print(f"2. Параметры в .env: {Config.get_db_params()}")
        print("3. PostgreSQL запущен")

if __name__ == "__main__":
    test_connection()
