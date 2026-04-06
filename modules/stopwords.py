"""
modules/stopwords.py
====================
Фильтр стоп-слов для SEO-ядра.

Содержит встроенный словарь нерелевантных слов для русскоязычного SEO:
- Информационный мусор (форум, скачать, торрент...)
- Брендовые запросы конкурентов (настраивается)
- Гео нерелевантные регионы
- Технические стоп-слова
- Поддержка пользовательского списка
"""

import re
from pathlib import Path
from typing import List, Set, Optional
from loguru import logger


# ─── Встроенный словарь стоп-слов ────────────────────────────────────────────

BUILTIN_STOPWORDS: Set[str] = {
    # Информационный мусор
    "скачать", "бесплатно", "торрент", "crack", "keygen", "serial",
    "ключ", "активация", "взлом", "кряк",

    # Форумы, соцсети
    "форум", "отзыв", "отзывы", "комментарии", "обсуждение",
    "вконтакте", "вк", "инстаграм", "ютуб", "youtube", "telegram",

    # Не коммерческий интент (для товарных ниш)
    "фото", "картинка", "картинки", "изображение", "png", "jpg",
    "wikipedia", "вики", "педия",

    # Нежелательные типы страниц
    "вакансия", "вакансии", "работа", "резюме", "устроиться",
    "диплом", "реферат", "курсовая", "решебник", "гдз",

    # Подержанное / б/у (если продаёте новое)
    # "б/у", "бу", "подержанный", "авито",  # раскомментируйте при необходимости

    # Временные маркеры (устаревшие годы — настройте)
    "2015", "2016", "2017", "2018", "2019", "2020",

    # Нерелевантные действия
    "своими руками", "самостоятельно", "diy", "ремонт своими",
    "починить", "отремонтировать",
}

# Стоп-паттерны (регулярные выражения)
STOP_PATTERNS = [
    r"\bскачать\b",
    r"\bбесплатн\w*\b",
    r"\bфорум\b",
    r"\bвики\w*\b",
    r"\d{4}\s*год",            # "2020 год"
    r"\bкак\s+сделать\b",
    r"\bчто\s+такое\b",        # информационный интент (опционально)
    r"\bопределение\b",
    r"\bзначение\s+слова\b",
]


class StopwordsFilter:
    """
    Фильтрует ключевые слова по стоп-словам и паттернам.

    Args:
        extra_stopwords:   Дополнительные стоп-слова
        extra_patterns:    Дополнительные регулярные выражения
        custom_file:       Путь к файлу со стоп-словами (одно слово в строке)
        min_freq:          Минимальная частотность для включения
        min_words:         Минимальное кол-во слов в запросе
        max_words:         Максимальное кол-во слов в запросе
    """

    def __init__(
        self,
        extra_stopwords: Optional[List[str]] = None,
        extra_patterns: Optional[List[str]] = None,
        custom_file: Optional[str] = None,
        min_words: int = 1,
        max_words: int = 7,
    ):
        self.stopwords = set(BUILTIN_STOPWORDS)
        self.patterns = list(STOP_PATTERNS)
        self.min_words = min_words
        self.max_words = max_words

        if extra_stopwords:
            self.stopwords.update(w.lower().strip() for w in extra_stopwords)

        if extra_patterns:
            self.patterns.extend(extra_patterns)

        if custom_file and Path(custom_file).exists():
            with open(custom_file, encoding="utf-8") as f:
                self.stopwords.update(line.strip().lower() for line in f if line.strip())
            logger.info(f"Загружен файл стоп-слов: {custom_file}")

        self._compiled = [re.compile(p, re.IGNORECASE | re.UNICODE) for p in self.patterns]

    def filter(self, keywords: List[str]) -> List[str]:
        """
        Фильтрует список ключей.

        Returns:
            Очищенный список без нерелевантных запросов
        """
        clean = []
        removed = 0
        for kw in keywords:
            if self._should_remove(kw):
                removed += 1
            else:
                clean.append(kw)

        logger.debug(f"Стоп-слова: удалено {removed} из {len(keywords)}")
        return clean

    def filter_with_reason(self, keywords: List[str]) -> tuple:
        """
        Фильтрует с указанием причины удаления.

        Returns:
            (clean_list, removed_dict {keyword: reason})
        """
        clean = []
        removed = {}
        for kw in keywords:
            reason = self._remove_reason(kw)
            if reason:
                removed[kw] = reason
            else:
                clean.append(kw)
        return clean, removed

    def _should_remove(self, keyword: str) -> bool:
        return bool(self._remove_reason(keyword))

    def _remove_reason(self, keyword: str) -> Optional[str]:
        kw_lower = keyword.lower().strip()
        words = kw_lower.split()

        # Длина запроса
        if len(words) < self.min_words:
            return "слишком короткий"
        if len(words) > self.max_words:
            return "слишком длинный"

        # Стоп-слова
        for word in words:
            if word in self.stopwords:
                return f"стоп-слово: {word}"

        # Стоп-паттерны
        for pattern in self._compiled:
            if pattern.search(kw_lower):
                return f"паттерн: {pattern.pattern}"

        return None

    def add_stopwords(self, words: List[str]):
        """Добавляет стоп-слова в рантайме."""
        self.stopwords.update(w.lower().strip() for w in words)

    def save_custom(self, path: str):
        """Сохраняет текущий словарь в файл."""
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(self.stopwords)))
        logger.info(f"Стоп-слова сохранены → {path}")
