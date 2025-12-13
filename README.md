## Описание:
Данный проект - Бот для анализа статистики видео, который понимает естественно-языковые запросы на русском и возвращает числовые ответы из базы данных.
Бот умеет отвечать на вопросы вида (примеры):
- «Сколько всего видео есть в системе?»
- «Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?»
- «Сколько видео набрало больше 100 000 просмотров за всё время?»
- «На сколько просмотров в сумме выросли все видео 28 ноября 2025?»
- «Сколько разных видео получали новые просмотры 27 ноября 2025?»

### Технологии:
- Язык программирования — Python
- База данных — PostgreSQL
- Telegram-бот — aiogram
- Обработка естественного языка: кастомный NLP-процессор на основе регулярных выражений

### Команда для запуска проекта локально: python main.py

### Команда для запуска теста проекта: python tests/test_nlp.py

### Команда для запуска теста PostgreSQL: python tests/test_postgres.py

### Шаги развертывания
1. Клонировать репозиторий и перейти в него в командной строке: git clone git@github.com:Tleuberdina/rlt_project.git
2. Cоздать и активировать виртуальное окружение:
   #### cd rlt_project
   #### python -m venv venv
   #### source venv/Scripts/activate
4. Установить зависимости из файла requirements.txt:
   #### python -m pip install -- upgrade pip
   #### pip install -r requirements.txt
5. Укажите в файле config/.env данные:
   #### TELEGRAM_BOT_TOKEN=ваш_токен
   #### DB_HOST=localhost
   #### DB_PORT=5432
   #### DB_NAME=video_stats
   #### DB_USER=postgres
   #### DB_PASSWORD=ваш_пароль_в_postgres
6. Создать базу данных:
   #### psql -U postgres -c "CREATE DATABASE video_stats;"
7. Создать таблицы в базе данных:
   #### psql -U postgres -d video_stats -f database/schema.sql
8. Загрузите данные из JSON (из файла videos.json в data/)
   #### python database/loader.py
