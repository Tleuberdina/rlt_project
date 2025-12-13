import logging
import asyncio
import re
from datetime import date
from typing import Optional, Tuple
import calendar
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from bot.nlp_processor import NLPProcessor, ParsedQuery
from database.query_manager import QueryManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VideoStatsBot:
    def __init__(self, token: str):
        self.token = token
        self.bot = Bot(
            token=token, 
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher(storage=MemoryStorage())
        self.nlp = NLPProcessor()
        self.query_manager = QueryManager()
        self._register_handlers()
    
    def _register_handlers(self):
        """Регистрация обработчиков команд."""
        self.dp.message.register(self.start_handler, Command(commands=["start"]))
        self.dp.message.register(self.help_handler, Command(commands=["help"]))
        self.dp.message.register(self.message_handler)
    
    def _extract_month_year_from_text(self, text: str) -> Optional[Tuple[date, date]]:
        """Извлечение месяца и года из текста запроса."""
        #import re
        #import calendar
        #from datetime import date
        #from typing import Optional, Tuple
    
        text_lower = text.lower()
    
        # Месяцы на русском
        month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
            'январе': 1, 'феврале': 2, 'марте': 3, 'апреле': 4,
            'мае': 5, 'июне': 6, 'июле': 7, 'августе': 8,
            'сентябре': 9, 'октябре': 10, 'ноябре': 11, 'декабре': 12
        }
    
        # Ищем любой месяц и год
        for month_name, month_num in month_map.items():
            # Паттерны: "в июне 2025", "за июль 2024", "июня 2025 года"
            patterns = [
                rf'в\s+{month_name}\s+(\d{{4}})',
                rf'за\s+{month_name}\s+(\d{{4}})',
                rf'{month_name}\s+(\d{{4}})\s+года',
                rf'{month_name}\s+(\d{{4}})',
            ]
        
            for pattern in patterns:
                match = re.search(pattern, text_lower)
                if match:
                    try:
                        year = int(match.group(1))
                        last_day = calendar.monthrange(year, month_num)[1]
                    
                        start_date = date(year, month_num, 1)
                        end_date = date(year, month_num, last_day)
                    
                        logger.info(f"📅 Извлечен {month_name} {year}: {start_date} - {end_date}")
                        return start_date, end_date
                    except Exception as e:
                        logger.error(f"Ошибка извлечения даты: {e}")
                        continue
    
        return None

    def _format_total_views_response(self, start_date: date, end_date: date, total_views: int) -> str:
        """Форматирование ответа для суммарных просмотров."""
        # Месяцы на русском в предложном падеже
        month_names = {
            1: 'январе', 2: 'феврале', 3: 'марте', 4: 'апреле',
            5: 'мае', 6: 'июне', 7: 'июле', 8: 'августе',
            9: 'сентябре', 10: 'октябре', 11: 'ноябре', 12: 'декабре'
        }
    
        if start_date.month == end_date.month and start_date.year == end_date.year:
            # Весь месяц
            month_name = month_names[start_date.month]
            period_text = f"в {month_name} {start_date.year} года"
        elif start_date.day == 1 and end_date.day in [28, 29, 30, 31]:
            # Вероятно, весь месяц
            if start_date.month == end_date.month:
                month_name = month_names[start_date.month]
                period_text = f"в {month_name} {start_date.year} года"
            else:
                period_text = f"с {start_date} по {end_date}"
        else:
            period_text = f"с {start_date} по {end_date}"
    
        return f"{total_views}"

    async def start_handler(self, message: types.Message):
        """Обработчик команды /start."""
        welcome_text = """
        🎬 <b>Бот статистики видео</b>
        
        Я могу ответить на вопросы о статистике видео.
        
        📊 <b>Примеры запросов:</b>
        • Сколько всего видео есть в системе?
        • Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025?
        • Сколько видео набрало больше 100000 просмотров?
        • На сколько просмотров в сумме выросли все видео 28 ноября 2025?
        • Сколько разных видео получали новые просмотры 27 ноября 2025?
        
        Просто напишите вопрос в чат!
        """
        await message.answer(welcome_text)
    
    async def help_handler(self, message: types.Message):
        """Обработчик команды /helpю"""
        help_text = """
        🤖 <b>Помощь по боту:</b>
        
        <b>Доступные команды:</b>
        /start - Начало работы
        /help - Эта справка
        
        <b>Формат вопросов:</b>
        • Используйте естественный русский язык
        • Даты можно указывать как "28 ноября 2025" или "с 1 по 5 ноября 2025"
        • В ответе вы получите одно число
        
        <b>Примеры:</b>
        • "Сколько всего видео?"
        • "Видео креатора id 123 за ноябрь 2025"
        • "Видео с >50000 просмотров"
        • "Прирост просмотров за вчера"
        """
        await message.answer(help_text)
    
    async def message_handler(self, message: types.Message):
        """Обработчик текстовых сообщений."""
        user_query = message.text
        logger.info(f"Получен запрос: {user_query}")
        
        try:
            # Распознаем намерение пользователя
            parsed_query = self.nlp.parse_query(user_query)
            logger.info(f"🎯 Распознан интент: {parsed_query.intent}")
            logger.info(f"📊 Параметры: {parsed_query.parameters}")
            
            # Обрабатываем запрос
            response = await self._process_parsed_query(parsed_query)
            
            # Отправляем ответ
            await message.answer(response)
            logger.info(f"📤 Отправлен ответ: {response[:50]}...")
            
        except Exception as e:
            logger.error(f"Ошибка обработки запроса: {e}")
            error_msg = (
                "❌ Произошла ошибка при обработке запроса.\n"
                "Попробуйте переформулировать вопрос или проверьте корректность данных."
            )
            await message.answer(error_msg)
    
    async def _process_parsed_query(self, parsed_query: ParsedQuery) -> str:
        """Обработка распарсенного запроса."""
        
        if parsed_query.intent == "total_videos":
            count = self.query_manager.get_total_videos()
            return f"{count:,}"

        elif parsed_query.intent == "total_views_period":
            start_date = parsed_query.parameters.get("start_date")
            end_date = parsed_query.parameters.get("end_date")
        
            logger.info(f"📊 Суммарные просмотры за период: start_date={start_date}, end_date={end_date}")
        
            if not start_date or not end_date:
                # Пробуем извлечь даты из оригинального запроса более универсально
                logger.info(f"⚠️ Даты не найдены в параметрах, парсим из запроса...")
            
                # Используем NLP процессор для парсинга даты из запроса
                dates = self.nlp._parse_dates_from_query(parsed_query.original_query)
            
                if dates:
                    start_date, end_date = dates
                    logger.info(f"📅 Найдены даты в запросе: {start_date} - {end_date}")
                else:
                    # Пробуем найти месяц и год в тексте
                    month_year = self._extract_month_year_from_text(parsed_query.original_query)
                    if month_year:
                        start_date, end_date = month_year
                        logger.info(f"📅 Найден месяц и год: {start_date} - {end_date}")
                    else:
                        return "❌ Не указан период. Пример: 'Суммарные просмотры за июнь 2025' или 'Сколько просмотров набрали все видео в марте 2024'"
        
            total_views = self.query_manager.get_total_views_for_period(start_date, end_date)
        
            # Форматируем красивый ответ
            response = self._format_total_views_response(start_date, end_date, total_views)
            return response

        elif parsed_query.intent == "negative_views_snapshots":
            count = self.query_manager.get_negative_views_snapshots_count()
            return f"{count:,}"
    
        elif parsed_query.intent == "videos_by_creator":
            creator_id = parsed_query.parameters.get("creator_id")
            start_date = parsed_query.parameters.get("start_date")
            end_date = parsed_query.parameters.get("end_date")
            logger.info(f"🔍 Поиск видео для creator_id={creator_id}")
            logger.info(f"📅 start_date={start_date}, end_date={end_date}")
            logger.info(f"📅 Тип start_date={type(start_date)}, тип end_date={type(end_date)}")
            
            if not creator_id:
                return "❌ Не указан ID креатора. Пример: 'Сколько видео у креатора с id user123?'"
            
            count = self.query_manager.get_videos_by_creator(
                creator_id, start_date, end_date
            )
            try:
                conn = self.query_manager._get_connection()
                cursor = conn.cursor()
        
                # Выполняем тот же запрос что и в get_videos_by_creator
                query = "SELECT id, video_created_at FROM videos WHERE creator_id = %s"
                params = [creator_id]
        
                if start_date:
                    query += " AND DATE(video_created_at) >= %s"
                    params.append(start_date)
        
                if end_date:
                    query += " AND DATE(video_created_at) <= %s"
                    params.append(end_date)
        
                cursor.execute(query, params)
                videos = cursor.fetchall()
        
                logger.info(f"📊 Найдено видео: {videos}")
                logger.info(f"📊 Всего записей: {len(videos)}")
        
                cursor.close()
                conn.close()
            except Exception as e:
                logger.error(f"Ошибка при проверке запроса: {e}")
            date_info = ""
            if start_date and end_date:
                date_info = f" за период с {start_date} по {end_date}"
            elif start_date:
                date_info = f" начиная с {start_date}"
            elif end_date:
                date_info = f" до {end_date}"
            
            return f"{count:,}"
        
        elif parsed_query.intent == "videos_by_views":
            min_views = parsed_query.parameters.get("min_views", 100000)
            count = self.query_manager.get_videos_with_views_above(min_views)
            return f"{count:,}"
        
        elif parsed_query.intent == "total_growth":
            target_date = parsed_query.parameters.get("date")
    
            if not target_date:
                # Пытаемся получить последнюю дату из данных
                try:
                    # Проверяем есть ли данные вообще
                    conn = self.query_manager._get_connection()
                    cursor = conn.cursor()
                    cursor.execute("SELECT MAX(DATE(created_at)) FROM video_snapshots WHERE delta_views_count > 0")
                    result = cursor.fetchone()
                    cursor.close()
                    conn.close()
            
                    if result and result[0]:
                        target_date = result[0]
                    else:
                        return "❌ В данных нет информации о приросте просмотров"
                
                except Exception as e:
                    logger.error(f"Ошибка при получении даты: {e}")
                    return "❌ Не удалось определить дату для анализа"
    
            growth = self.query_manager.get_total_views_growth_on_date(target_date)
    
            if growth == 0:
                return f"📊 За {target_date} не было зафиксировано прироста просмотров"
    
            return f"{growth:,}"
        
        elif parsed_query.intent == "unique_growth":
            target_date = parsed_query.parameters.get("date")
            if not target_date:
                return "❌ Не указана дата. Пример: 'Сколько видео получали просмотры вчера?'"
            
            count = self.query_manager.get_unique_videos_with_growth_on_date(target_date)
            return f"{count:,}"
        
        elif parsed_query.intent == "videos_by_creator_with_views":
            creator_id = parsed_query.parameters.get("creator_id")
            min_views = parsed_query.parameters.get("min_views", 10000)
    
            if not creator_id:
                return "❌ Не указан ID креатора. Пример: 'Сколько видео у креатора с id abc123 набрало больше 10000 просмотров?'"
    
            count = self.query_manager.get_videos_by_creator_with_views(creator_id, min_views)
            return f"{count:,}"
        
        else:
            # Для unknown запросов даем подсказки
            suggestions = ""
            if "автор" in parsed_query.original_query.lower() or "креатор" in parsed_query.original_query.lower():
                suggestions = "Укажите ID креатора, например: 'Сколько видео у креатора с id abc123'"
            elif "просмотры" in parsed_query.original_query.lower():
                suggestions = "Уточните запрос, например: 'Прирост просмотров за вчера' или 'Видео с более 10000 просмотров'"
            return (
                "🤔 Не удалось распознать запрос.\n\n"
                "Попробуйте один из примеров:\n"
                "• Сколько всего видео?\n"
                "• Видео креатора id 123\n"
                "• Видео с >50000 просмотров\n"
                "• Прирост просмотров за вчера"
            )
    
    async def run(self):
        """Асинхронный запуск бота."""
        try:
            logger.info("Запускаем бота...")
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            raise
        finally:
            await self.bot.session.close()
