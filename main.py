import os
import asyncio
import sys
from config.config import Config


async def main_async():
    """Асинхронная главная функция"""
    try:
        # Проверяем конфигурацию
        Config.validate()
        from bot.handlers import VideoStatsBot
        
        print("🚀 Запуск бота статистики видео...")
        bot = VideoStatsBot(Config.TELEGRAM_BOT_TOKEN)
        await bot.run()
        
    except ValueError as e:
        print(f"❌ Ошибка конфигурации: {e}")
        print("\nСоздайте файл config/.env с содержимым:")
        print("TELEGRAM_BOT_TOKEN=ваш_токен_бота")
        print("DB_HOST=localhost")
        print("DB_PORT=5432")
        print("DB_NAME=video_stats")
        print("DB_USER=postgres")
        print("DB_PASSWORD=ваш_пароль_postgres")
    except ImportError as e:
        print(f"❌ Ошибка импорта: {e}")
        print("Установите зависимости: pip install -r requirements.txt")
    except KeyboardInterrupt:
        print("\n⏹️ Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
