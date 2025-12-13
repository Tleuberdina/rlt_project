import psycopg2
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
import os
from config.config import Config


class QueryManager:
    """Менеджер запросов к базе данных."""
    
    def __init__(self):
        self.conn_params = Config.get_db_params()
    
    def _get_connection(self):
        """Получение соединения с базой данных."""
        return psycopg2.connect(**self.conn_params)
    
    def get_total_videos(self) -> int:
        """Сколько всего видео есть в системе?"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM videos")
                result = cursor.fetchone()
                return result[0] if result else 0
        finally:
            conn.close()
    
    def get_videos_by_creator(self, creator_id: str, 
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None) -> int:
        """Сколько видео у креатора за период."""
        conn = self._get_connection()
        try:
            query = "SELECT COUNT(*) FROM videos WHERE creator_id = %s"
            params = [creator_id]
            
            if start_date:
                query += " AND video_created_at >= %s"
                params.append(datetime.combine(start_date, datetime.min.time()))
            
            if end_date:
                query += " AND video_created_at <= %s"
                params.append(datetime.combine(end_date, datetime.max.time()))
            
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result[0] if result else 0
        finally:
            conn.close()
    
    def get_videos_with_views_above(self, min_views: int) -> int:
        """Сколько видео набрало больше X просмотров."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM videos WHERE views_count > %s",
                    [min_views]
                )
                result = cursor.fetchone()
                return result[0] if result else 0
        finally:
            conn.close()
    
    def get_total_views_growth_on_date(self, target_date: date) -> int:
        """На сколько просмотров в сумме выросли все видео за дату."""
        conn = self._get_connection()
        try:
            start_datetime = datetime.combine(target_date, datetime.min.time())
            end_datetime = datetime.combine(target_date, datetime.max.time())
            
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COALESCE(SUM(delta_views_count), 0)
                    FROM video_snapshots
                    WHERE created_at >= %s AND created_at <= %s
                """, [start_datetime, end_datetime])
                
                result = cursor.fetchone()
                return int(result[0]) if result else 0
        finally:
            conn.close()
    
    def get_unique_videos_with_growth_on_date(self, target_date: date) -> int:
        """Сколько разных видео получали новые просмотры за дату."""
        conn = self._get_connection()
        try:
            start_datetime = datetime.combine(target_date, datetime.min.time())
            end_datetime = datetime.combine(target_date, datetime.max.time())
            
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(DISTINCT video_id)
                    FROM video_snapshots
                    WHERE created_at >= %s 
                    AND created_at <= %s
                    AND delta_views_count > 0
                """, [start_datetime, end_datetime])
                
                result = cursor.fetchone()
                return result[0] if result else 0
        finally:
            conn.close()
    
    def execute_custom_query(self, sql: str, params: tuple = None) -> Optional[int]:
        """Выполнение произвольного SQL запроса."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            print(f"Ошибка выполнения запроса: {e}")
            return None
        finally:
            conn.close()
