"""
SEO Keyword Parser
==================
Точка входа. Запускает полный цикл сбора семантического ядра:
  1. Яндекс.Wordstat (Selenium)
  2. Google Trends (pytrends)
  3. LSI-кластеризация
  4. Анализ конкурентов из ТОП SERP
  5. Экспорт в Excel + CSV

Использование:
  pip install -r requirements.txt
  cp .env.example .env   # заполнить своими данными
  python main.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from modules.yandex_wordstat import YandexWordstat
from modules.google_trends import GoogleTrendsParser
from modules.lsi_clustering import LSIClusterer
from modules.competitor_parser import CompetitorParser
from modules.stopwords import StopwordsFilter
from modules.exporter import KeywordExporter
from modules.serpstat import SerpstatParser  # опционально

load_dotenv()

# ─── Настройки ───────────────────────────────────────────────────────────────

SEED_KEYWORDS = [
    "купить ноутбук",
    "ноутбук для работы",
    "игровой ноутбук",
    "ноутбук недорого",
    "лучший ноутбук 2024",
]

CONFIG = {
    "yandex_login":    os.getenv("YANDEX_LOGIN", ""),
    "yandex_password": os.getenv("YANDEX_PASSWORD", ""),
    "region_id":       int(os.getenv("REGION_ID", 213)),   # 213=Москва, 2=СПб
    "serpstat_key":    os.getenv("SERPSTAT_API_KEY", ""),
    "wordstat_pages":  int(os.getenv("WORDSTAT_PAGES", 3)),
    "serp_top_n":      int(os.getenv("SERP_TOP_N", 10)),
    "clusters_n":      int(os.getenv("CLUSTERS_N", 15)),
    "output_dir":      os.getenv("OUTPUT_DIR", "output"),
    "headless":        os.getenv("HEADLESS", "true").lower() == "true",
}

# ─── Логирование ─────────────────────────────────────────────────────────────

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add("logs/parser_{time:YYYY-MM-DD}.log", rotation="1 day", retention="7 days")

Path("logs").mkdir(exist_ok=True)
Path(CONFIG["output_dir"]).mkdir(exist_ok=True)


# ─── Главная функция ─────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("SEO Keyword Parser — старт")
    logger.info(f"Семена: {SEED_KEYWORDS}")
    logger.info(f"Регион: {CONFIG['region_id']} | Страниц Wordstat: {CONFIG['wordstat_pages']}")
    logger.info("=" * 60)

    all_wordstat_results = []

    # ── Шаг 1: Яндекс.Wordstat ──────────────────────────────────────────────
    if CONFIG["yandex_login"]:
        logger.info("Шаг 1/5 → Яндекс.Wordstat")
        ws = YandexWordstat(
            login=CONFIG["yandex_login"],
            password=CONFIG["yandex_password"],
            region_id=CONFIG["region_id"],
            headless=CONFIG["headless"],
        )
        try:
            for seed in SEED_KEYWORDS:
                results = ws.get_keywords(seed, pages=CONFIG["wordstat_pages"])
                all_wordstat_results.extend(results)
                logger.success(f"  Wordstat «{seed}» → {len(results)} ключей")
        finally:
            ws.close()
    else:
        logger.warning("Шаг 1/5 → Wordstat пропущен (YANDEX_LOGIN не задан)")

    # ── Шаг 2: Serpstat API (опционально) ───────────────────────────────────
    serpstat_results = []
    if CONFIG["serpstat_key"]:
        logger.info("Шаг 1b → Serpstat API")
        sp = SerpstatParser(api_key=CONFIG["serpstat_key"])
        for seed in SEED_KEYWORDS:
            data = sp.get_keywords(seed, se="y", limit=200)
            serpstat_results.extend(data)
            logger.success(f"  Serpstat «{seed}» → {len(data)} ключей")
    else:
        logger.info("Serpstat пропущен (ключ не задан)")

    # Объединяем все ключевые слова в единый список строк
    raw_keywords = list({r.keyword for r in all_wordstat_results})
    raw_keywords += [r["keyword"] for r in serpstat_results if r["keyword"] not in raw_keywords]

    # Если Wordstat и Serpstat недоступны — используем сиды для демонстрации
    if not raw_keywords:
        logger.warning("Нет данных из Wordstat/Serpstat — работаем на seed-ключах")
        raw_keywords = SEED_KEYWORDS

    # ── Шаг 3: Фильтрация стоп-слов ─────────────────────────────────────────
    logger.info("Шаг 2/5 → Фильтрация стоп-слов")
    sf = StopwordsFilter()
    keywords_clean = sf.filter(raw_keywords)
    logger.success(f"  {len(raw_keywords)} → {len(keywords_clean)} ключей после фильтрации")

    # ── Шаг 4: Google Trends — сезонность ───────────────────────────────────
    logger.info("Шаг 3/5 → Google Trends (сезонность)")
    gt = GoogleTrendsParser(geo="RU")
    seasonal_data = {}
    # GT позволяет сравнивать макс. 5 ключей за раз
    sample = keywords_clean[:20]
    try:
        seasonal_data = gt.get_seasonal_keywords(sample)
        rising = gt.get_rising_queries(SEED_KEYWORDS[0])
        logger.success(f"  Сезонность для {len(seasonal_data)} ключей получена")
        logger.info(f"  Растущие тренды: {rising[:5]}")
    except Exception as e:
        logger.warning(f"  Google Trends недоступен: {e}")

    # ── Шаг 5: Анализ конкурентов ───────────────────────────────────────────
    logger.info("Шаг 4/5 → Парсинг конкурентов из ТОП SERP")
    comp = CompetitorParser()
    competitor_data = {}
    serp_urls_map = {}  # для SERP-кластеризации

    for seed in SEED_KEYWORDS[:3]:  # ограничиваем для скорости
        try:
            urls = comp.get_serp_urls(seed, engine="yandex", top_n=CONFIG["serp_top_n"])
            serp_urls_map[seed] = urls
            page_data = []
            for url in urls[:5]:  # парсим первые 5 страниц
                info = comp.extract_page_keywords(url)
                if info:
                    page_data.append(info)
            competitor_data[seed] = page_data
            logger.success(f"  Конкуренты «{seed}» → {len(page_data)} страниц проанализировано")
        except Exception as e:
            logger.warning(f"  Ошибка парсинга конкурентов «{seed}»: {e}")

    # ── Шаг 6: LSI-кластеризация ────────────────────────────────────────────
    logger.info("Шаг 5/5 → LSI-кластеризация")
    clusterer = LSIClusterer(n_clusters=min(CONFIG["clusters_n"], len(keywords_clean)))
    clusters = {}
    if len(keywords_clean) >= 3:
        # SERP-кластеризация если есть данные
        if serp_urls_map:
            clusters = clusterer.cluster_by_serp(keywords_clean, serp_urls_map)
        else:
            clusters = clusterer.cluster_by_tfidf(keywords_clean)
        logger.success(f"  Получено {len(clusters)} кластеров")
    else:
        logger.warning("  Недостаточно ключей для кластеризации")

    # ── Экспорт результатов ─────────────────────────────────────────────────
    logger.info("Экспорт результатов")
    exp = KeywordExporter(output_dir=CONFIG["output_dir"])

    xlsx_path = exp.to_excel(
        wordstat_results=all_wordstat_results,
        keywords_clean=keywords_clean,
        seasonal_data=seasonal_data,
        clusters=clusters,
        competitor_data=competitor_data,
    )
    csv_path = exp.to_csv(keywords_clean, seasonal_data)

    logger.success(f"Excel → {xlsx_path}")
    logger.success(f"CSV   → {csv_path}")
    logger.info("=" * 60)
    logger.success("Готово!")


if __name__ == "__main__":
    main()
