import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from config.config import Config


def get_db_connection():
    """Создание подключения к базе данных."""
    params = Config.get_db_params()
    return psycopg2.connect(**params)

def recreate_tables():
    """Пересоздание таблиц с правильной схемой."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Удаляем старые таблицы если есть
        cursor.execute("DROP TABLE IF EXISTS video_snapshots CASCADE")
        cursor.execute("DROP TABLE IF EXISTS videos CASCADE")
        
        # Создаем таблицы заново
        cursor.execute("""
            CREATE TABLE videos (
                id VARCHAR(255) PRIMARY KEY,
                creator_id VARCHAR(255) NOT NULL,
                video_created_at TIMESTAMP NOT NULL,
                views_count INTEGER DEFAULT 0,
                likes_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                reports_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE video_snapshots (
                snapshot_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255) NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
                views_count INTEGER DEFAULT 0,
                likes_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                reports_count INTEGER DEFAULT 0,
                delta_views_count INTEGER DEFAULT 0,
                delta_likes_count INTEGER DEFAULT 0,
                delta_comments_count INTEGER DEFAULT 0,
                delta_reports_count INTEGER DEFAULT 0,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Создаем индексы
        cursor.execute("CREATE INDEX idx_videos_creator_id ON videos(creator_id)")
        cursor.execute("CREATE INDEX idx_videos_created_at ON videos(video_created_at)")
        cursor.execute("CREATE INDEX idx_videos_views ON videos(views_count)")
        cursor.execute("CREATE INDEX idx_snapshots_video_id ON video_snapshots(video_id)")
        cursor.execute("CREATE INDEX idx_snapshots_created_at ON video_snapshots(created_at)")
        
        conn.commit()
        print("✅ Таблицы пересозданы с правильной схемой")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при создании таблиц: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def load_json_to_db(json_file_path: str):
    """Загрузка данных из JSON файла в базу данных."""
    # Проверяем конфигурацию
    Config.validate()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Чтение JSON файла
        print(f"📖 Чтение файла: {json_file_path}")
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Извлекаем список видео
        if isinstance(data, dict) and 'videos' in data:
            videos_list = data['videos']
        else:
            videos_list = data
        
        print(f"✅ Найдено {len(videos_list)} видео")
        
        # Подготовка данных
        videos_data = []
        snapshots_data = []
        
        for i, video in enumerate(videos_list, 1):
            # Видео
            videos_data.append((
                video['id'],
                video['creator_id'],
                video['video_created_at'],
                video.get('views_count', 0),
                video.get('likes_count', 0),
                video.get('comments_count', 0),
                video.get('reports_count', 0)
            ))
            
            # Снапшоты
            for snapshot in video.get('snapshots', []):
                snapshots_data.append((
                    snapshot.get('id'),  # snapshot_id
                    video['id'],          # video_id
                    snapshot.get('views_count', 0),
                    snapshot.get('likes_count', 0),
                    snapshot.get('comments_count', 0),
                    snapshot.get('reports_count', 0),
                    snapshot.get('delta_views_count', 0),
                    snapshot.get('delta_likes_count', 0),
                    snapshot.get('delta_comments_count', 0),
                    snapshot.get('delta_reports_count', 0),
                    snapshot.get('created_at')
                ))
            
            # Прогресс
            if i % 100 == 0:
                print(f"  📦 Обработано {i} видео...")
        
        print(f"📊 Подготовлено: {len(videos_data)} видео, {len(snapshots_data)} снапшотов")
        
        # Вставка видео
        print("💾 Вставка видео...")
        execute_values(
            cursor,
            """
            INSERT INTO videos 
            (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                views_count = EXCLUDED.views_count,
                likes_count = EXCLUDED.likes_count,
                comments_count = EXCLUDED.comments_count,
                reports_count = EXCLUDED.reports_count,
                updated_at = CURRENT_TIMESTAMP
            """,
            videos_data
        )
        
        # Вставка снапшотов
        print("💾 Вставка снапшотов...")
        execute_values(
            cursor,
            """
            INSERT INTO video_snapshots 
            (snapshot_id, video_id, views_count, likes_count, comments_count, reports_count,
             delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at)
            VALUES %s
            ON CONFLICT (snapshot_id) DO UPDATE SET
                views_count = EXCLUDED.views_count,
                likes_count = EXCLUDED.likes_count,
                comments_count = EXCLUDED.comments_count,
                reports_count = EXCLUDED.reports_count,
                delta_views_count = EXCLUDED.delta_views_count,
                delta_likes_count = EXCLUDED.delta_likes_count,
                delta_comments_count = EXCLUDED.delta_comments_count,
                delta_reports_count = EXCLUDED.delta_reports_count,
                updated_at = CURRENT_TIMESTAMP
            """,
            snapshots_data
        )
        
        conn.commit()
        print(f"🎉 УСПЕХ! Загружено: {len(videos_data)} видео и {len(snapshots_data)} снапшотов")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # 1. Пересоздаем таблицы с правильной схемой
    recreate_tables()
    
    # 2. Загружаем данные
    json_file = "data/videos.json"
    if os.path.exists(json_file):
        load_json_to_db(json_file)
    else:
        print(f"❌ Файл не найден: {json_file}")
