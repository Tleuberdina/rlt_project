import psycopg2
from datetime import date, datetime, time, timedelta
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
        """Сколько видео у креатора за период (по дате публикации видео)."""
        conn = self._get_connection()
        try:
            query = "SELECT COUNT(*) FROM videos WHERE creator_id = %s"
            params = [creator_id]
        
            if start_date:
                # Используем DATE() для сравнения только дат (включительно)
                query += " AND DATE(video_created_at) >= %s"
                params.append(start_date)
        
            if end_date:
                # Используем DATE() для сравнения только дат (включительно)
                query += " AND DATE(video_created_at) <= %s"
                params.append(end_date)
        
            # ДЛЯ ОТЛАДКИ: выведем запрос
            print(f"🔍 SQL запрос: {query}")
            print(f"🔍 Параметры: {params}")
        
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result[0] if result else 0
        finally:
            conn.close()

    def get_total_views_growth_for_creator_with_time_period(self, creator_id: str, 
                                                       target_date: date,
                                                       start_time: time,
                                                       end_time: time) -> int:
        """На сколько просмотров суммарно выросли все видео креатора в указанный временной интервал."""
        conn = self._get_connection()
        try:
            # Создаем полные datetime объекты
            start_datetime = datetime.combine(target_date, start_time)
            end_datetime = datetime.combine(target_date, end_time)
        
            print(f"DEBUG: Запрос для креатора {creator_id}")
            print(f"DEBUG: Период: {start_datetime} - {end_datetime}")
        
            query = """
                SELECT COALESCE(SUM(vs.delta_views_count), 0)
                FROM video_snapshots vs
                JOIN videos v ON vs.video_id = v.id
                WHERE v.creator_id = %s
                AND vs.created_at >= %s
                AND vs.created_at <= %s
                AND vs.delta_views_count > 0
            """
        
            with conn.cursor() as cursor:
                cursor.execute(query, (creator_id, start_datetime, end_datetime))
                result = cursor.fetchone()
                total_growth = int(result[0]) if result else 0
            
                # Дополнительная диагностика
                cursor.execute("""
                    SELECT vs.video_id, vs.created_at, vs.delta_views_count
                    FROM video_snapshots vs
                    JOIN videos v ON vs.video_id = v.id
                    WHERE v.creator_id = %s
                    AND vs.created_at >= %s
                    AND vs.created_at <= %s
                    AND vs.delta_views_count > 0
                    ORDER BY vs.created_at
                """, (creator_id, start_datetime, end_datetime))
            
                snapshots = cursor.fetchall()
                print(f"DEBUG: Найдено {len(snapshots)} снапшотов с ростом просмотров")
                for video_id, created_at, delta in snapshots:
                    print(f"  - {video_id}: {created_at} (+{delta})")
            
                print(f"DEBUG: Итоговый суммарный рост: {total_growth}")
            
                return total_growth
        finally:
            conn.close()

    def get_total_views_for_period(self, start_date: date, end_date: date) -> int:
        """Суммарное количество просмотров всех видео за период."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COALESCE(SUM(views_count), 0)
                    FROM videos 
                    WHERE DATE(video_created_at) >= %s 
                    AND DATE(video_created_at) <= %s
                """, [start_date, end_date])
            
                result = cursor.fetchone()
                return int(result[0]) if result else 0
        finally:
            conn.close()

    def get_negative_views_snapshots_count(self) -> int:
        """Сколько замеров статистики с отрицательными просмотрами."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Ищем снапшоты где delta_views_count < 0
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM video_snapshots 
                    WHERE delta_views_count < 0
                """)
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

    def get_videos_by_creator_with_views(self, creator_id: str, min_views: int) -> int:
        """Сколько видео у креатора набрало больше X просмотров"""
        conn = self._get_connection()
        try:
            query = """
                SELECT COUNT(*) 
                FROM videos 
                WHERE creator_id = %s AND views_count > %s
            """
        
            with conn.cursor() as cursor:
                cursor.execute(query, (creator_id, min_views))
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
