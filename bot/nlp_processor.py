import re
from datetime import datetime, date, time, timedelta
import calendar
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass

@dataclass
class ParsedQuery:
    """Распарсенный запрос пользователя."""
    intent: str  # total_videos, videos_by_creator, etc.
    parameters: Dict[str, Any]
    original_query: str

    def get(self, key: str, default=None):
        """Получить параметр по ключу."""
        return self.parameters.get(key, default)


class NLPProcessor:
    """Процессор естественного языка без LLM."""
    
    def __init__(self):
        self.month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }

    def _parse_time_from_query(self, query: str) -> Optional[Tuple[time, time]]:
        """Парсинг временного интервала из запроса."""
        # Паттерны для времени:
        # "с 10:00 до 15:00"
        # "с 10 до 15 часов"
        # "с 10 утра до 15 дня"
        # "между 10:00 и 15:00"
        
        query_lower = query.lower()
        
        # 1. Паттерн с двоеточием: "с 10:00 до 15:00"
        time_pattern_colon = r'с\s+(\d{1,2}:\d{2})\s+до\s+(\d{1,2}:\d{2})'
        match = re.search(time_pattern_colon, query_lower)
        if match:
            try:
                start_time = datetime.strptime(match.group(1), '%H:%M').time()
                end_time = datetime.strptime(match.group(2), '%H:%M').time()
                return start_time, end_time
            except ValueError:
                pass
        
        # 2. Паттерн без двоеточия: "с 10 до 15 часов"
        time_pattern_hours = r'с\s+(\d{1,2})\s+(?:до|по)\s+(\d{1,2})\s*(?:часов|ч\.?)'
        match = re.search(time_pattern_hours, query_lower)
        if match:
            try:
                start_hour = int(match.group(1))
                end_hour = int(match.group(2))
                start_time = time(start_hour, 0)
                end_time = time(end_hour, 0)
                return start_time, end_time
            except ValueError:
                pass
        
        # 3. Простой паттерн: "с 10 до 15"
        time_pattern_simple = r'с\s+(\d{1,2})\s+(?:до|по)\s+(\d{1,2})'
        match = re.search(time_pattern_simple, query_lower)
        if match:
            try:
                start_hour = int(match.group(1))
                end_hour = int(match.group(2))
                start_time = time(start_hour, 0)
                end_time = time(end_hour, 0)
                return start_time, end_time
            except ValueError:
                pass
        
        return None

    def parse_query(self, query: str) -> ParsedQuery:
        """Основной метод парсинга запроса."""
        query_lower = query.lower().strip()

        # ПРИОРИТЕТ 1: Новый запрос о суммарных просмотрах ВСЕХ видео
        if self._match_total_views_all_videos_period(query_lower):
            return self._parse_total_views_all_videos_period(query_lower, query)
    
        # ПРИОРИТЕТ 2: Запрос с временным интервалом для конкретного креатора
        if self._match_total_views_with_time_period(query_lower):
            return self._parse_total_views_with_time_period(query_lower, query)
    
        # ПРИОРИТЕТ 3: Точные совпадения по паттернам

        # 1. Суммарные просмотры за период
        total_views_period_match = self._match_total_views_period(query_lower)
        if total_views_period_match:
            return ParsedQuery(
                intent="total_views_period",
                parameters=total_views_period_match,
                original_query=query
            )
        # 2. Сколько всего есть замеров статистики с отрицательными просмотрами
        negative_views_match = self._match_negative_views(query_lower)
        if negative_views_match:
            return ParsedQuery(
                intent="negative_views_snapshots",
                parameters={},
                original_query=query
            )
        # 3. Сколько видео у креатора с id X набрали больше Y просмотров?
        combined_match = self._match_creator_with_views(query_lower)
        if combined_match:
            return ParsedQuery(
                intent="videos_by_creator_with_views",
                parameters=combined_match,
                original_query=query
            )

        # 4. Сколько всего видео есть в системе?
        if self._match_total_videos(query_lower):
            return ParsedQuery(
                intent="total_videos",
                parameters={},
                original_query=query
            )
        
        # 5. Сколько видео у креатора с id ... вышло с ... по ...?
        creator_match = self._match_creator_videos(query_lower)
        if creator_match:
            return ParsedQuery(
                intent="videos_by_creator",
                parameters=creator_match,
                original_query=query
            )
        
        # 6. Сколько видео набрало больше X просмотров?
        views_match = self._match_videos_by_views(query_lower)
        if views_match:
            return ParsedQuery(
                intent="videos_by_views",
                parameters=views_match,
                original_query=query
            )
        
        # 7. На сколько просмотров в сумме выросли все видео X?
        growth_match = self._match_total_growth(query_lower)
        if growth_match:
            return ParsedQuery(
                intent="total_growth",
                parameters=growth_match,
                original_query=query
            )
        
        # 8. Сколько разных видео получали новые просмотры X?
        unique_match = self._match_unique_videos_growth(query_lower)
        if unique_match:
            return ParsedQuery(
                intent="unique_growth",
                parameters=unique_match,
                original_query=query
            )
        
        # ПРИОРИТЕТ 4: Расширенный анализ по ключевым словам
        return self._advanced_analysis(query_lower, query)

    def _match_total_views_all_videos_period(self, query: str) -> bool:
        """Определяет, является ли запрос о суммарных просмотрах всех видео за период."""
        keywords = {
            'суммарное количество просмотров',
            'суммарное число просмотров',
            'сколько всего просмотров',
            'сумма просмотров',
            'набрали все видео',
            'всего просмотров набрали'
        }
    
        # Проверяем наличие ключевых слов про ВСЕ видео
        has_all_keywords = any(keyword in query for keyword in [
            'все видео', 'всех видео', 'любого автора', 'любого креатора'
        ]) or ('креатора' not in query and 'автора' not in query and 'id' not in query)
    
        has_views_keywords = any(keyword in query for keyword in keywords)
        has_date_period = bool(self._parse_dates_from_query(query))
    
        return has_views_keywords and has_date_period and has_all_keywords

    def _parse_total_views_all_videos_period(self, query: str, original_query: str) -> ParsedQuery:
        """Парсинг запроса о суммарных просмотрах всех видео за период."""
        params = {}
    
        # Парсим даты
        dates = self._parse_dates_from_query(query)
        if dates:
            params["start_date"] = dates[0]
            params["end_date"] = dates[1] if len(dates) > 1 else dates[0]
    
        return ParsedQuery(
            intent="total_views_all_videos_period",
            parameters=params,
            original_query=original_query
        )

    def _match_total_views_with_time_period(self, query: str) -> bool:
        """Определяет, является ли запрос о суммарном росте просмотров с временным интервалом."""
        keywords = {
            'просмотров суммарно выросли',
            'суммарно выросли все видео',
            'сложить изменения просмотров',
            'изменения просмотров между замерами',
            'замерами попадающими в этот интервал'
        }
        
        # Проверяем наличие ключевых слов
        has_keywords = any(keyword in query for keyword in keywords)
        
        # Проверяем наличие временного интервала
        has_time_period = bool(self._parse_time_from_query(query))
        
        # Проверяем наличие ID креатора
        has_creator_id = bool(re.search(r'id\s+([a-f0-9]{32}|[a-f0-9-]{36}|\w+)', query, re.IGNORECASE))
        
        # Проверяем наличие даты
        has_date = bool(self._parse_dates_from_query(query))
        
        return has_keywords and has_time_period and has_creator_id and has_date

    def _parse_month_year_from_query(self, query: str) -> Optional[Tuple[date, date]]:
        """Парсинг месяца и года из запроса."""
        # Паттерны: "в июне 2025 года", "за июнь 2025", "июне 2025"
        month_pattern = r'(' + '|'.join(self.month_map.keys()) + r')\s+(\d{4})'
        match = re.search(month_pattern, query)
    
        if match:
            month_name = match.group(1)
            year = int(match.group(2))
            month = self.month_map[month_name]
        
            # Первый день месяца
            start_date = date(year, month, 1)
        
            # Последний день месяца
            if month == 12:
                end_date = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = date(year, month + 1, 1) - timedelta(days=1)
        
            return start_date, end_date
    
        return None

    def _parse_total_views_with_time_period(self, query: str, original_query: str) -> ParsedQuery:
        """Парсинг запроса о суммарном росте просмотров с временным интервалом."""
        params = {}
        
        # 1. Ищем ID креатора
        id_match = re.search(r'id\s+([a-f0-9]{32}|[a-f0-9-]{36}|\w+)', query, re.IGNORECASE)
        if id_match:
            params["creator_id"] = id_match.group(1)
        
        # 2. Парсим даты
        dates = self._parse_dates_from_query(query)
        if dates:
            params["start_date"] = dates[0]
            params["end_date"] = dates[1] if len(dates) > 1 else dates[0]
        
        # 3. Парсим временной интервал
        time_period = self._parse_time_from_query(query)
        if time_period:
            params["start_time"] = time_period[0]
            params["end_time"] = time_period[1]
        
        return ParsedQuery(
            intent="total_views_period",
            parameters=params,
            original_query=original_query
        )

    def _match_total_views_period(self, query: str) -> Optional[Dict[str, Any]]:
        """Какое суммарное количество просмотров набрали все видео за период."""
        query_lower = query.lower()
    
        print(f"🔍 Анализ запроса для total_views_period: {query_lower}")  # Отладка
    
         # Базовые проверки
        has_total_keywords = any(word in query_lower for word in [
            'суммарное', 'сумма', 'общее', 'всего', 'набрали все', 'все видео'
        ])
        has_views_keywords = any(word in query_lower for word in [
            'просмотров', 'просмотры'
        ])
    
        if not (has_total_keywords and has_views_keywords):
            return None
    
        # Парсим период
        period = self.parse_date_period(query_lower)
        if period:
            return period
    
        # Даже если не нашли точный период, но запрос явно про суммарные просмотры
        if 'суммарное количество просмотров' in query_lower:
            # Попробуем найти год
            year_match = re.search(r'\b(202[0-9])\b', query_lower)
            if year_match:
                year = int(year_match.group(1))
                return {
                    "start_date": date(year, 1, 1),
                    "end_date": date(year, 12, 31)
                }
    
        return None

    def _match_negative_views(self, query: str) -> Optional[Dict[str, Any]]:
        """Сколько всего есть замеров статистики с отрицательными просмотрами?"""
        query_lower = query.lower()
    
        # Ключевые слова для этого запроса
        negative_keywords = [
            'отрицательн',  # отрицательных, отрицательное
            'уменьшилось',
            'стало меньше',
            'по сравнению с предыдущим',
            'просмотров за час',
            'замеров статистики',
            'количество просмотров стало меньше'
        ]
    
        # Проверяем наличие ключевых слов
        has_negative = any(keyword in query_lower for keyword in negative_keywords)
        has_snapshots = any(word in query_lower for word in ['замеров', 'замеры', 'снапшотов', 'статистик'])
        has_views = any(word in query_lower for word in ['просмотров', 'просмотры'])
    
        # Конкретные паттерны
        patterns = [
            r'сколько всего есть замеров статистики.*отрицательн',
            r'замеров.*отрицательн.*просмотров',
            r'просмотров за час.*отрицательн',
            r'количество просмотров стало меньше',
            r'по сравнению с предыдущим.*меньше'
        ]
    
        # Проверяем точные паттерны
        for pattern in patterns:
            if re.search(pattern, query_lower):
                return {}
    
        # Проверяем комбинацию ключевых слов
        if has_negative and has_snapshots and has_views:
            return {}
    
        return None

    def _match_creator_with_views(self, query: str) -> Optional[Dict[str, Any]]:
        """Сколько видео у креатора с id X набрали больше Y просмотров?"""
        query_lower = query.lower()
    
        # 1. Ищем ID (32 hex символа)
        id_match = re.search(r'id\s+([a-f0-9]{32})', query_lower, re.IGNORECASE)
        if not id_match:
            return None
    
        creator_id = id_match.group(1)
    
        # 2. Удаляем ВЕСЬ ID из запроса (не только сам ID, но и "id ")
        query_without_id_full = re.sub(r'id\s+' + re.escape(creator_id), '', query_lower, flags=re.IGNORECASE)
    
        # 3. Проверяем что запрос содержит слова про просмотры
        has_views_keywords = any(word in query_without_id_full for word in ['просмотров', 'просмотры', 'набрали', 'набрало'])
        has_comparison = any(word in query_without_id_full for word in ['больше', 'более', 'свыше', '>'])
    
        # Если нет ключевых слов про просмотры/сравнение, это не наш запрос
        if not (has_views_keywords and has_comparison):
            return None
    
        # 4. Ищем число просмотров (убираем пробелы в числах)
        # Заменяем "10 000" на "10000" во всем запросе
        query_clean = re.sub(r'(\d)\s+(\d)', r'\1\2', query_without_id_full)
    
        # Ищем числа после индикаторов
        indicators = ['больше', 'более', 'свыше', '>', 'набрали', 'набрало']
    
        for indicator in indicators:
            pattern = rf'{indicator}\s*(\d+)'
            match = re.search(pattern, query_clean)
            if match:
                min_views = int(match.group(1))
                # Проверяем что это разумное число просмотров (не 1061 из ID)
                if min_views >= 1000:  # Минимум 1000 просмотров
                    return {
                        "creator_id": creator_id,
                    "   min_views": min_views
                    }
    
        return None

    def _match_total_videos(self, query: str) -> bool:
        """Сколько всего видео есть в системе?"""
        patterns = [
            r'^сколько всего видео',
            r'^сколько видео в системе',
            r'^общее количество видео',
            r'^всего видео$',
            r'^количество всех видео',
            r'^сколько роликов в системе',
            r'^суммарное количество видео',
            r'^сколько у вас видео',
            r'^сколько всего роликов',
            r'^общее число видео'
        ]
        return any(re.search(pattern, query) for pattern in patterns)
    
    def _match_creator_videos(self, query: str) -> Optional[Dict[str, Any]]:
        """Сколько видео у креатора с id ... вышло с ... по ...(без условий про просмотры)?"""
        # Сначала проверяем что это НЕ запрос с условием по просмотрам
        query_lower = query.lower()
    
        # Если есть слова про просмотры и сравнение, это не простой запрос
        has_views = any(word in query_lower for word in ['просмотров', 'просмотры', 'набрали', 'набрало'])
        has_comparison = any(word in query_lower for word in ['больше', 'более', 'свыше', '>'])
    
        if has_views and has_comparison:
            return None
        # Ищем конкретные паттерны с ID
        id_patterns = [
            r'креатор(?:а|ом)?\s+(?:с\s+)?id\s+([a-f0-9]{32})',
            r'автор(?:а|ом)?\s+(?:с\s+)?id\s+([a-f0-9]{32})',
            r'id\s+([a-f0-9]{32})\s+креатор',
            r'id\s+([a-f0-9]{32})\s+автор',
            r'у\s+креатор(?:а|а\s+с\s+id)?\s+([a-f0-9]{32})',
            r'у\s+автор(?:а|а\s+с\s+id)?\s+([a-f0-9]{32})',
            r'креатор\s+([a-f0-9]{32})',
            r'автор\s+([a-f0-9]{32})',
            r'креатор\s+с\s+id\s+([a-f0-9]{32})',
            r'автор\s+с\s+id\s+([a-f0-9]{32})'
        ]
        
        creator_id = None
        for pattern in id_patterns:
            match = re.search(pattern, query)
            if match:
                creator_id = match.group(1)
                print(f"🔍 Найден ID: {creator_id} по паттерну: {pattern}")  # Отладка
                break
        
        # Если нашли ID, проверяем что это не общий запрос про видео
        if creator_id and creator_id not in ['автора', 'креатора', 'автор', 'креатор']:
            # Проверяем что ID не просто слово "автора" или "креатора"
            if creator_id.lower() in ['автора', 'креатора', 'автор', 'креатор', 'у']:
                return None
            
            # Парсим даты
            dates = self._parse_dates_from_query(query)
            
            # Проверяем что запрос действительно про креатора, а не общий
            if any(word in query for word in ['уникальн', 'разных', 'новые', 'прирост', 'вырос']):
                return None
            
            return {
                "creator_id": creator_id,
                "start_date": dates[0] if dates else None,
                "end_date": dates[1] if dates else None
            }
        
        return None
    
    def _match_videos_by_views(self, query: str) -> Optional[Dict[str, Any]]:
        """Сколько видео набрало больше X просмотров?"""
        # Сначала точные паттерны с числами
        patterns = [
            r'больше\s+([\d\s]+)\s+просмотров',
            r'набрало\s+([\d\s]+)\s+просмотров',
            r'>\s*([\d\s]+)\s*просмотров',
            r'превысило\s+([\d\s]+)\s+просмотров',
            r'свыше\s+([\d\s]+)\s+просмотров',
            r'более\s+([\d\s]+)\s+просмотров',
            r'видео\s+с\s+([\d\s]+)\s+просмотрами',
            r'видео\s+([\d\s]+)\s+просмотров'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query)
            if match:
                number_str = match.group(1).replace(' ', '').replace(',', '')
                try:
                    min_views = int(number_str)
                    return {"min_views": min_views}
                except ValueError:
                    continue
        
        # Общие паттерны про просмотры
        if re.search(r'сколько видео.*просмотров', query) or \
           re.search(r'видео.*просмотров.*сколько', query):
            # Пробуем извлечь число из запроса
            numbers = re.findall(r'\d+', query.replace(' ', '').replace(',', ''))
            if numbers:
                return {"min_views": int(numbers[-1])}
            return {"min_views": 100000}  # Значение по умолчанию
        
        return None
    
    def _match_total_growth(self, query: str) -> Optional[Dict[str, Any]]:
        """На сколько просмотров в сумме выросли все видео X?"""
        # Паттерны для общего прироста
        growth_patterns = [
            r'на сколько просмотров.*выросли',
            r'суммарный прирост просмотров',
            r'сумма просмотров.*выросла',
            r'прирост просмотров',
            r'сколько просмотров.*прибавилось',
            r'общий прирост.*просмотров',
            r'насколько.*выросли.*просмотры',
            r'выросло.*просмотров.*сколько',
            r'прирост.*за.*вчера',
            r'прирост.*за.*сегодня',
            r'прирост.*за.*неделю',
            r'новые просмотры.*за.*недел',
            r'просмотры.*за.*недел',
            r'сколько.*просмотров.*за.*недел'
        ]
        
        for pattern in growth_patterns:
            if re.search(pattern, query):
                dates = self._parse_dates_from_query(query)
                if dates:
                    return {"date": dates[0]}
                else:
                    # Если дата не указана, проверяем контекст
                    if any(word in query for word in ['вчера', 'сегодня', 'завтра', 'недел', 'месяц']):
                        dates = self._parse_dates_from_query(query)
                        if dates:
                            return {"date": dates[0]}
                    return {"date": datetime.now().date()}
        
        return None
    
    def _match_unique_videos_growth(self, query: str) -> Optional[Dict[str, Any]]:
        """Сколько разных видео получали новые просмотры X?"""
        # Паттерны для уникальных видео с приростом
        unique_patterns = [
            r'сколько разных видео.*просмотры',
            r'уникальных видео.*новые просмотры',
            r'разных видео.*получали просмотры',
            r'сколько видео.*новые просмотры',
            r'видео.*получало.*просмотры',
            r'какие видео.*просмотры',
            r'уникальные видео.*просмотры',
            r'разные видео.*просмотры',
            r'видео.*получали.*новые',
            r'новые просмотры.*видео',
            r'какие.*видео.*просмотры',
            r'видео.*получали.*просмотры'
        ]
        
        for pattern in unique_patterns:
            if re.search(pattern, query):
                # Дополнительная проверка: если есть "новые просмотры" но нет "уникальных"/"разных"
                # и есть "за неделю" - это скорее total_growth
                if 'новые просмотры' in query and 'за неделю' in query:
                    if not any(word in query for word in ['уникальн', 'разных', 'разные', 'какие']):
                        continue  # Пропускаем, это total_growth
                
                dates = self._parse_dates_from_query(query)
                if dates:
                    return {"date": dates[0]}
                else:
                    # Если есть слова указывающие на уникальность/разные
                    if any(word in query for word in ['уникальн', 'разных', 'разные', 'какие', 'получали']):
                        return {"date": datetime.now().date()}
        
        return None

    def _parse_month_year_from_text(self, query: str) -> Optional[Dict[str, Any]]:
        """Парсинг месяца и года из текста запроса."""
        query_lower = query.lower()
    
        print(f"🔍 Парсим месяц и год из: {query_lower}")  # Отладка
    
        # Проверяем все падежи месяцев
        month_variants = {
            'января': 1, 'январе': 1,
            'февраля': 2, 'феврале': 2,
            'марта': 3, 'марте': 3,
            'апреля': 4, 'апреле': 4,
            'мая': 5, 'мае': 5,
            'июня': 6, 'июне': 6,
            'июля': 7, 'июле': 7,
            'августа': 8, 'августе': 8,
            'сентября': 9, 'сентябре': 9,
            'октября': 10, 'октябре': 10,
            'ноября': 11, 'ноябре': 11,
            'декабря': 12, 'декабре': 12
        }
    
        for month_name, month_num in month_variants.items():
            # Ищем "в июне 2025" или "июня 2025 года"
            patterns = [
                rf'в\s+{month_name}\s+(\d{{4}})\s*года?',
                rf'{month_name}\s+(\d{{4}})\s*года?',
                rf'за\s+{month_name}\s+(\d{{4}})',
            ]
        
            for pattern in patterns:
                match = re.search(pattern, query_lower)
                if match:
                    try:
                        year = int(match.group(1))
                        import calendar
                        last_day = calendar.monthrange(year, month_num)[1]
                    
                        start_date = date(year, month_num, 1)
                        end_date = date(year, month_num, last_day)
                    
                        print(f"✅ Распарсен: {month_name} {year} -> {start_date} - {end_date}")
                        return {
                            "start_date": start_date,
                            "end_date": end_date
                        }
                    except Exception as e:
                        print(f"❌ Ошибка парсинга: {e}")
                        continue
    
        # Если не нашли, пробуем просто найти год
        year_match = re.search(r'\b(20\d{2})\b', query_lower)
        if year_match:
            year = int(year_match.group(1))
        
            # Проверяем, есть ли указание на месяц в запросе
            month_words = ['месяц', 'месяца', 'месяце']
            if any(word in query_lower for word in month_words):
                # Если есть слово "месяц", но не указан конкретный месяц
                # ищем по контексту
                if 'июн' in query_lower:
                    month_num = 6
                elif 'июл' in query_lower:
                    month_num = 7
                elif 'авг' in query_lower:
                    month_num = 8
                else:
                    # Если месяц не указан, возвращаем весь год
                    return {
                        "start_date": date(year, 1, 1),
                        "end_date": date(year, 12, 31)
                    }
            
                import calendar
                last_day = calendar.monthrange(year, month_num)[1]
            
                start_date = date(year, month_num, 1)
                end_date = date(year, month_num, last_day)
            
                print(f"✅ Найден по контексту: {year} месяц {month_num}")
                return {
                    "start_date": start_date,
                    "end_date": end_date
                }
    
        return None

    def _parse_dates_from_query(self, query: str) -> Optional[Tuple[date, date]]:
        """Парсинг дат из запроса."""
        today = datetime.now().date()
        
        # Сначала проверяем относительные даты
        if "вчера" in query:
            yesterday = today - timedelta(days=1)
            return yesterday, yesterday
        elif "сегодня" in query:
            return today, today
        elif "завтра" in query:
            tomorrow = today + timedelta(days=1)
            return tomorrow, tomorrow
        elif "неделю" in query or "недели" in query:
            week_ago = today - timedelta(days=7)
            return week_ago, today
        elif "месяц" in query or "месяца" in query:
            month_ago = today - timedelta(days=30)
            return month_ago, today
        
        # Проверяем диапазон: "с 1 по 5 ноября 2025"
        range_pattern = r'с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+(' + '|'.join(self.month_map.keys()) + r')\s+(\d{4})'
        range_match = re.search(range_pattern, query)
        if range_match:
            day_start = int(range_match.group(1))
            day_end = int(range_match.group(2))
            month_name = range_match.group(3)
            year = int(range_match.group(4))
        
            month = self.month_map[month_name]
            start_date = date(year, month, day_start)
            end_date = date(year, month, day_end)
        
            return start_date, end_date
    
        # Проверяем одиночную дату: "28 ноября 2025"
        single_pattern = r'(\d{1,2})\s+(' + '|'.join(self.month_map.keys()) + r')\s+(\d{4})'
        single_match = re.search(single_pattern, query)
        if single_match:
            day = int(single_match.group(1))
            month_name = single_match.group(2)
            year = int(single_match.group(3))
        
            month = self.month_map[month_name]
            d = date(year, month, day)
            return d, d
    
        # Проверяем месяц и год: "в июне 2025"
        month_year = self._parse_month_year_from_query(query)
        if month_year:
            return month_year
    
        # Пытаемся найти дату в формате ГГГГ-ММ-ДД
        iso_pattern = r'(\d{4})-(\d{1,2})-(\d{1,2})'
        iso_match = re.search(iso_pattern, query)
        if iso_match:
            year = int(iso_match.group(1))
            month = int(iso_match.group(2))
            day = int(iso_match.group(3))
            d = date(year, month, day)
            return d, d
    
        return None
    
    def _advanced_analysis(self, query_lower: str, original_query: str) -> ParsedQuery:
        """Расширенный анализ запроса с весами ключевых слов."""
        print(f"🔍 Расширенный анализ запроса: {query_lower}")
        # Веса для разных типов запросов (обновленные)
        keyword_weights = {
            "total_videos": {
                "сколько": 3, "всего": 3, "всех": 2, "общее": 2, 
                "роликов": 1, "количество": 2, "суммарное": 1, "число": 1
            },
            "videos_by_creator": {
                "креатор": 2, "автор": 2, "id": 4, "у": 1, 
                "создатель": 2, "user": 1, "юзера": 1
            },
            "videos_by_views": {
                "просмотров": 3, "больше": 2, "набрало": 2, "превысило": 2,
                "свыше": 2, "более": 2, ">": 3, "просмотрами": 2
            },
            "total_growth": {
                "прирост": 4, "выросли": 3, "прибавилось": 3, "увеличились": 2,
                "насколько": 1, "суммарный": 3, "общий": 2, "за": 1,
                "новые": 2, "просмотры": 2
            },
            "unique_growth": {
                "уникальн": 5, "разных": 5, "разные": 5, "новые": 1,
                "получали": 3, "получало": 3, "отдельных": 2, "различных": 2,
                "какие": 3
            },
            "total_views_all_videos_period": {
                "суммарное": 3, "суммарный": 3, "сумма": 2, "всех": 3,
                "все": 3, "любого": 2, "любые": 2, "опубликован": 2,
                "набрали": 2, "просмотров": 3, "количество": 2
            },
            "total_views_period": {
                "суммарное": 8, "сумма": 7, "общее": 6, "всего": 5,
                "количество просмотров": 9, "просмотров набрали": 8,
                "все видео": 7, "опубликован": 6, "набрали": 7,
                "январ": 4, "феврал": 4, "март": 4, "апрел": 4,
                "май": 4, "июн": 4, "июл": 4, "август": 4,
                "сентябр": 4, "октябр": 4, "ноябр": 4, "декабр": 4,
                "месяц": 5, "2025": 4, "2024": 4, "года": 4
            },
            "negative_views_snapshots": {
                "отрицательн": 5, "уменьшилось": 4, "меньше": 3,
                "замеров": 4, "снапшотов": 4, "статистики": 3,
                "просмотров": 3, "час": 2, "предыдущим": 3,
                "по сравнению": 3
            },
            "videos_by_creator_with_views": {
                "креатор": 2, "автор": 2, "id": 3, "у": 1,
                "просмотров": 3, "больше": 2, "набрали": 2, "набрало": 2,
                "просмотрами": 2, "итоговой": 1, "статистике": 1
            }
        }
        
        # Подсчет весов
        scores = {intent: 0 for intent in keyword_weights.keys()}
        
        for intent, weights in keyword_weights.items():
            for keyword, weight in weights.items():
                if keyword in query_lower:
                    scores[intent] += weight
        
        # Специальные правила для проблемных случаев
        
        # Специальное правило для total_views_period
        if 'суммарное количество просмотров' in query_lower:
            scores["total_views_period"] += 10

        # Специальное правило: если есть "все видео" и дата, но нет ID
        if 'все видео' in query_lower and not ('креатора' in query_lower or 'автора' in query_lower):
            # Проверяем есть ли дата
            if self._parse_dates_from_query(query_lower):
                scores["total_views_all_videos_period"] += 5

        # Специальные правила для комбинированных запросов
        if 'креатора' in query_lower and 'просмотров' in query_lower:
            # Проверяем есть ли условия с "больше" или числа
            if 'больше' in query_lower or any(word.isdigit() for word in query_lower.split()):
                scores["videos_by_creator_with_views"] += 5
                scores["videos_by_creator"] = max(0, scores["videos_by_creator"] - 2)

        # 1. "Сколько видео у автора" - без ID должно быть unknown
        if 'сколько видео' in query_lower and 'у автора' in query_lower:
            # Проверяем есть ли ID
            id_match = re.search(r'[a-f0-9]{32}|[a-f0-9-]{36}|\bid\s+\w+', query_lower)
            if not id_match:
                scores["videos_by_creator"] = 0  # Обнуляем, если нет ID
                scores["unknown"] = 5  # Увеличиваем unknown
        
        # 2. "Новые просмотры за неделю" - должно быть total_growth, а не unique_growth
        if 'новые просмотры' in query_lower and 'недел' in query_lower:
            if not any(word in query_lower for word in ['уникальн', 'разных', 'разные', 'какие']):
                # Увеличиваем total_growth, уменьшаем unique_growth
                scores["total_growth"] += 5
                scores["unique_growth"] = max(0, scores["unique_growth"] - 3)
        
        # 3. Если есть "сколько" но нет других ключевых слов - unknown
        if 'сколько' in query_lower and sum(scores.values()) < 3:
            scores["unknown"] = scores.get("unknown", 0) + 3
        
        # Исключаем videos_by_creator если нет ID
        if scores["videos_by_creator"] > 0:
            # Проверяем есть ли реальный ID (не слова "автор", "креатор")
            has_real_id = bool(re.search(r'[a-f0-9]{32}|[a-f0-9-]{36}|\bid\s+[a-z0-9]', query_lower))
            if not has_real_id and 'автор' in query_lower:
                # Это просто "у автора" без ID
                scores["videos_by_creator"] = 0
        
        # Находим лучший интент
        best_intent = max(scores, key=scores.get)
        best_score = scores[best_intent]
        
        if best_intent == "total_views_period" and best_score > 5:
        # Пробуем найти месяц и год
            month_year = self._parse_month_year_from_text(query_lower)
            if month_year:
                return ParsedQuery(
                    intent="total_views_period",
                    parameters=month_year,
                    original_query=original_query
                )

        # Если лучший score слишком низкий, считаем unknown
        if best_score < 2:
            return ParsedQuery(
                intent="unknown",
                parameters={"query": original_query},
                original_query=original_query
            )
        
        # Извлекаем параметры
        params = {}
        
        if best_intent == "videos_by_creator_with_views":
            # Ищем ID креатора
            id_match = re.search(r'[a-f0-9]{32}|[a-f0-9-]{36}|\bid\s+([^\s]+)', query_lower)
            if id_match:
                creator_id = id_match.group(1) if id_match.groups() else id_match.group(0)
                if creator_id.lower() not in ['креатора', 'автора', 'креатор', 'автор', 'id']:
                    params["creator_id"] = creator_id
            
            # Ищем количество просмотров
            numbers = re.findall(r'\d+', query_lower.replace(' ', '').replace(',', ''))
            if numbers:
                params["min_views"] = int(numbers[-1])
            else:
                params["min_views"] = 10000

        if best_intent == "videos_by_creator":
            # Ищем ID (32 hex символа) - ОБНОВЛЕННЫЙ ПАТТЕРН
            id_match = re.search(r'[a-f0-9]{32}', query_lower)
            if id_match:
                creator_id = id_match.group(0)
                print(f"🔍 Найден ID в расширенном анализе: {creator_id}")
            
                if len(creator_id) == 32:
                    params["creator_id"] = creator_id
                else:
                    return ParsedQuery(
                        intent="unknown",
                        parameters={"query": original_query},
                        original_query=original_query
                    )
            else:
                return ParsedQuery(
                    intent="unknown",
                    parameters={"query": original_query},
                    original_query=original_query
                )
            
        elif best_intent == "videos_by_views":
            # Ищем число
            numbers = re.findall(r'\d+', query_lower.replace(' ', '').replace(',', ''))
            if numbers:
                params["min_views"] = int(numbers[-1])
            else:
                params["min_views"] = 100000
        
        elif best_intent in ["total_growth", "unique_growth"]:
            # Парсим дату
            dates = self._parse_dates_from_query(query_lower)
            if dates:
                params["date"] = dates[0]
            else:
                # По умолчанию сегодня
                params["date"] = datetime.now().date()
        
        return ParsedQuery(
            intent=best_intent,
            parameters=params,
            original_query=original_query
        )

    def parse_date_period(self, query: str) -> Optional[Dict[str, Any]]:
        """Парсинг периода дат из запроса."""
        # 1. Попробуем найти диапазон дат
        dates = self._parse_dates_from_query(query)
        if dates:
            return {
                "start_date": dates[0],
                "end_date": dates[1]
            }
        
        # 2. Попробуем найти месяц и год
        month_year = self._parse_month_year_from_text(query)
        if month_year:
            return month_year
        
        # 3. Попробуем найти год
        year_match = re.search(r'\b(20\d{2})\b', query)
        if year_match:
            year = int(year_match.group(1))
            
            # Если есть слово "год" или "года", это может быть весь год
            if 'год' in query.lower() and not any(month in query.lower() for month in self.month_map.keys()):
                return {
                    "start_date": date(year, 1, 1),
                    "end_date": date(year, 12, 31)
                }
        
        return None
