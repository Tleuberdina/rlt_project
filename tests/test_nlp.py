import sys
sys.path.append('.')
from bot.nlp_processor import NLPProcessor

def run_tests():
    """Запуск всех тестов"""
    nlp = NLPProcessor()
    
    test_cases = [
        ("Прирост просмотров за вчера", "total_growth"),
        ("Сколько всего видео", "total_videos"),
        ("Видео с более 50000 просмотров", "videos_by_views"),
        ("Уникальные видео с новыми просмотрами", "unique_growth"),
        ("Видео креатора id aca1061a9d324ecf8c3fa2bb32d7be63", "videos_by_creator"),
        ("Прирост за сегодня", "total_growth"),
        ("Сколько разных видео получали просмотры вчера", "unique_growth"),
        ("Видео с >100000 просмотров", "videos_by_views"),
        ("Ролики автора с id aca1061a9d324ecf8c3fa2bb32d7be63", "videos_by_creator"),
        ("Общий прирост просмотров", "total_growth"),
        ("Уникальные ролики с просмотрами", "unique_growth"),
        ("Разные видео получали просмотры сегодня", "unique_growth"),
        ("На сколько выросли просмотры вчера", "total_growth"),
        ("Сколько видео у автора", "unknown"),  # Нет ID - должно быть unknown
        ("Видео с просмотрами больше 1000", "videos_by_views"),
        ("Новые просмотры за неделю", "total_growth"),  # Исправлено: должно быть total_growth
        ("Сколько всего роликов", "total_videos"),
        ("Креатор с id aca1061a9d324ecf8c3fa2bb32d7be63", "videos_by_creator"),
        ("Уникальные видео", "unique_growth"),
        ("Прирост", "total_growth"),
    ]
    
    print("🧪 Финальное тестирование NLP процессора")
    print("=" * 60)
    
    passed = 0
    total = len(test_cases)
    
    for query, expected_intent in test_cases:
        result = nlp.parse_query(query)
        
        if result.intent == expected_intent:
            status = "✅"
            passed += 1
        else:
            status = "❌"
        
        print(f"{status} Запрос: '{query}'")
        print(f"   Ожидалось: {expected_intent:20} Получено: {result.intent}")
        if result.parameters:
            print(f"   Параметры: {result.parameters}")
        print()
    
    print(f"📊 Результат: {passed}/{total} ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    else:
        print(f"\n⚠️ Не прошло: {total - passed} тестов")
    
    return passed == total

def debug_problematic_queries():
    """Отладка проблемных запросов"""
    nlp = NLPProcessor()
    
    problematic = [
        "Сколько видео у автора",
        "Новые просмотры за неделю",
    ]
    
    print("\n🔍 Отладка проблемных запросов:")
    print("=" * 60)
    
    for query in problematic:
        print(f"\n📋 Запрос: '{query}'")
        result = nlp.parse_query(query)
        print(f"   Результат: {result.intent}")
        print(f"   Параметры: {result.parameters}")
        
        # Анализ ключевых слов
        query_lower = query.lower()
        keywords = {
            'сколько': 'total_videos/videos_by_creator',
            'видео': 'общее',
            'у': 'videos_by_creator', 
            'автора': 'videos_by_creator',
            'новые': 'total_growth/unique_growth',
            'просмотры': 'total_growth/unique_growth',
            'за': 'указание времени',
            'неделю': 'дата'
        }
        
        print("   Ключевые слова:")
        for kw, desc in keywords.items():
            if kw in query_lower:
                print(f"     - '{kw}': {desc}")

if __name__ == "__main__":
    success = run_tests()
    
    if not success:
        debug_problematic_queries()
