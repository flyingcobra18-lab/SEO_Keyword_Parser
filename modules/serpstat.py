"""
modules/serpstat.py
===================
Клиент для Serpstat API v4.

Serpstat — российский SEO-инструмент с хорошим покрытием Яндекса и Google.
Документация: https://serpstat.com/api/

Возможности:
- Сбор ключей с частотностью и CPC
- Похожие ключевые слова
- Данные о конкурентах домена
- История позиций

Для работы нужен платный план Serpstat с доступом к API.
"""

import time
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

import requests
from loguru import logger


@dataclass
class SerpstatKeyword:
    keyword: str
    region_queries_count: int = 0   # частотность в регионе
    cost: float = 0.0               # CPC в USD
    competition: float = 0.0       # конкуренция 0-1
    found_results: int = 0         # кол-во результатов в выдаче
    keyword_length: int = 0        # кол-во слов
    source: str = "serpstat"

    def to_dict(self) -> dict:
        return {
            "keyword":      self.keyword,
            "shows":        self.region_queries_count,
            "cpc":          self.cost,
            "competition":  round(self.competition, 2),
            "serp_results": self.found_results,
            "source":       self.source,
        }


class SerpstatParser:
    """
    Клиент Serpstat API.

    Args:
        api_key:     Ключ API (получить на serpstat.com)
        se:          Поисковик: 'y' — Яндекс, 'g' — Google
        default_se:  Код поисковой системы (y — Яндекс Россия)
    """

    API_URL = "https://api.serpstat.com/v4"

    # Коды поисковых систем Serpstat
    SEARCH_ENGINES = {
        "y":  "y",    # Яндекс Россия
        "g":  "g",    # Google Россия (google.ru)
        "gu": "gu",   # Google Украина
        "gb": "gb",   # Google Беларусь
    }

    def __init__(self, api_key: str, se: str = "y"):
        self.api_key = api_key
        self.se = se
        self.session = requests.Session()
        self._rate_limit_delay = 1.0  # секунды между запросами

    def _call(self, method: str, params: dict) -> Optional[dict]:
        """Базовый вызов API."""
        payload = {
            "id": 1,
            "method": method,
            "params": {
                "token": self.api_key,
                "se": self.se,
                **params,
            }
        }
        try:
            r = self.session.post(self.API_URL, json=payload, timeout=15)
            r.raise_for_status()
            data = r.json()

            if "error" in data:
                logger.warning(f"Serpstat ошибка: {data['error']}")
                return None

            return data.get("result", {})

        except Exception as e:
            logger.warning(f"Serpstat API недоступен: {e}")
            return None
        finally:
            time.sleep(self._rate_limit_delay)

    def get_keywords(
        self,
        query: str,
        limit: int = 200,
        sort_by: str = "region_queries_count",
        min_freq: int = 10,
    ) -> List[SerpstatKeyword]:
        """
        Получает ключевые слова для запроса.

        Args:
            query:    Исходный запрос
            limit:    Максимум ключей
            sort_by:  Поле сортировки
            min_freq: Минимальная частотность
        """
        result = self._call("SerpstatKeywordProcedure.getKeywords", {
            "query": query,
            "size": limit,
        })

        if not result:
            return []

        keywords = []
        for item in result.get("data", []):
            freq = item.get("region_queries_count", 0)
            if freq < min_freq:
                continue
            keywords.append(SerpstatKeyword(
                keyword=item.get("keyword", ""),
                region_queries_count=freq,
                cost=item.get("cost", 0.0),
                competition=item.get("competition", 0.0),
                found_results=item.get("found_results", 0),
                keyword_length=len(item.get("keyword", "").split()),
            ))

        logger.success(f"Serpstat «{query}» → {len(keywords)} ключей")
        return keywords

    def get_related_keywords(self, query: str, limit: int = 100) -> List[SerpstatKeyword]:
        """Похожие ключи (семантически связанные)."""
        result = self._call("SerpstatKeywordProcedure.getRelatedKeywords", {
            "query": query,
            "size": limit,
        })
        if not result:
            return []

        return [
            SerpstatKeyword(
                keyword=item.get("keyword", ""),
                region_queries_count=item.get("region_queries_count", 0),
                cost=item.get("cost", 0.0),
            )
            for item in result.get("data", [])
        ]

    def get_domain_keywords(self, domain: str, limit: int = 500) -> List[dict]:
        """
        Получает ключи, по которым ранжируется домен.
        Удобно для анализа конкурентов.
        """
        result = self._call("SerpstatDomainProcedure.getDomainKeywords", {
            "domain": domain,
            "size": limit,
        })
        if not result:
            return []

        return [
            {
                "keyword":   item.get("keyword", ""),
                "position":  item.get("position", 0),
                "url":       item.get("url", ""),
                "traffic":   item.get("traff", 0),
                "shows":     item.get("region_queries_count", 0),
            }
            for item in result.get("data", [])
        ]

    def get_competitors(self, domain: str) -> List[dict]:
        """Находит конкурентов домена по пересечению ключей."""
        result = self._call("SerpstatDomainProcedure.getCompetitors", {
            "domain": domain,
        })
        if not result:
            return []

        return [
            {
                "domain":      item.get("domain", ""),
                "relevance":   item.get("relevance", 0),
                "common_kw":   item.get("common_keywords", 0),
            }
            for item in result.get("data", [])
        ]

    def get_api_stats(self) -> Optional[dict]:
        """Проверяет баланс запросов API."""
        result = self._call("SerpstatUserProcedure.stats", {})
        return result
