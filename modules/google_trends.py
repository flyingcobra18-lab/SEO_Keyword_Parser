"""
modules/google_trends.py
========================
Модуль для работы с Google Trends через pytrends.

Возможности:
- Динамика интереса по времени (тренд)
- Сезонный анализ (пиковый и провальный месяц, коэффициент сезонности)
- Растущие и связанные запросы
- Региональный интерес по субъектам РФ
- Сравнение до 100 ключей через батч-нормализацию
- Автоповтор при ошибках (rate limit Trends API)
"""

import time
import random
from typing import List, Dict, Optional, Tuple

import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
from loguru import logger


# ─── Словари ──────────────────────────────────────────────────────────────────

MONTH_NAMES_RU = {
    1: "январь", 2: "февраль", 3: "март",    4: "апрель",
    5: "май",    6: "июнь",    7: "июль",    8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

SEASONALITY_LABELS = {
    (0.0, 1.5): "равномерный",
    (1.5, 2.5): "слабосезонный",
    (2.5, 4.0): "сезонный",
    (4.0, 99):  "высокосезонный",
}


def _season_label(ratio: float) -> str:
    for (lo, hi), label in SEASONALITY_LABELS.items():
        if lo <= ratio < hi:
            return label
    return "неизвестно"


# ─── Основной класс ───────────────────────────────────────────────────────────

class GoogleTrendsParser:
    """
    Обёртка над pytrends с дополнительной аналитикой для SEO.

    Параметры:
        geo:        Страна (ISO-3166), по умолчанию RU
        lang:       Язык интерфейса
        retries:    Кол-во повторов при ошибке 429
        backoff:    Начальная пауза при повторе (секунды)
    """

    def __init__(
        self,
        geo: str = "RU",
        lang: str = "ru",
        retries: int = 5,
        backoff: float = 60.0,
    ):
        self.geo = geo
        self.retries = retries
        self.backoff = backoff
        self.pt = TrendReq(hl=lang, tz=180, retries=retries, backoff_factor=1.5)

    # ── Динамика по времени ───────────────────────────────────────────────────

    def get_interest_over_time(
        self,
        keywords: List[str],
        timeframe: str = "today 12-m",
        geo: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Возвращает DataFrame с динамикой интереса.

        timeframe варианты:
            'now 1-H'    — последний час (реальное время)
            'now 4-H'    — последние 4 часа
            'now 1-d'    — последние сутки
            'now 7-d'    — последние 7 дней
            'today 1-m'  — последний месяц (понедельные данные)
            'today 3-m'  — последние 3 месяца
            'today 12-m' — последний год (еженедельные)
            'today 5-y'  — последние 5 лет (ежемесячные)
            '2020-01-01 2024-12-31' — произвольный период
        """
        keywords = keywords[:5]  # Google Trends: максимум 5 ключей
        self._build_payload(keywords, timeframe=timeframe, geo=geo or self.geo)

        df = self.pt.interest_over_time()
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        return df

    # ── Сезонный анализ ───────────────────────────────────────────────────────

    def get_seasonal_keywords(
        self, keywords: List[str]
    ) -> Dict[str, dict]:
        """
        Анализирует сезонность для списка ключей.

        Возвращает для каждого ключа:
            peak_month     — месяц пикового спроса
            low_month      — месяц минимального спроса
            ratio          — коэффициент сезонности (max/min)
            label          — текстовая метка (сезонный, равномерный и т.д.)
            monthly_avg    — средний интерес по месяцам {1: float, ...}
        """
        result = {}
        # Разбиваем на батчи по 5
        batches = [keywords[i:i+5] for i in range(0, len(keywords), 5)]

        for batch in batches:
            try:
                df = self.get_interest_over_time(batch, timeframe="today 5-y")
            except Exception as e:
                logger.warning(f"Trends ошибка (пропускаем батч): {e}")
                continue

            for kw in batch:
                if kw not in df.columns:
                    continue
                try:
                    monthly = df[kw].groupby(df.index.month).mean()
                    peak = int(monthly.idxmax())
                    low = int(monthly.idxmin())
                    ratio = float(monthly.max()) / (float(monthly.min()) + 1e-9)

                    result[kw] = {
                        "peak_month":    MONTH_NAMES_RU[peak],
                        "peak_month_n":  peak,
                        "low_month":     MONTH_NAMES_RU[low],
                        "low_month_n":   low,
                        "ratio":         round(ratio, 2),
                        "label":         _season_label(ratio),
                        "is_seasonal":   ratio > 2.5,
                        "monthly_avg":   {m: round(v, 1) for m, v in monthly.items()},
                    }
                except Exception as e:
                    logger.debug(f"Ошибка обработки {kw}: {e}")

            time.sleep(random.uniform(2, 4))

        return result

    # ── Растущие запросы ──────────────────────────────────────────────────────

    def get_rising_queries(self, keyword: str) -> List[str]:
        """Возвращает список быстро растущих запросов (Rising) для ключа."""
        try:
            self._build_payload([keyword])
            related = self.pt.related_queries()
            df_rising = related.get(keyword, {}).get("rising", pd.DataFrame())
            if df_rising is not None and not df_rising.empty:
                return df_rising["query"].tolist()
        except Exception as e:
            logger.debug(f"Rising queries ошибка: {e}")
        return []

    def get_related_queries(self, keyword: str) -> Dict[str, List[str]]:
        """Возвращает топ и растущие связанные запросы."""
        try:
            self._build_payload([keyword])
            related = self.pt.related_queries()
            kw_data = related.get(keyword, {})
            top = kw_data.get("top", pd.DataFrame())
            rising = kw_data.get("rising", pd.DataFrame())
            return {
                "top":    top["query"].tolist() if top is not None and not top.empty else [],
                "rising": rising["query"].tolist() if rising is not None and not rising.empty else [],
            }
        except Exception as e:
            logger.warning(f"Related queries ошибка: {e}")
            return {"top": [], "rising": []}

    # ── Региональный интерес ──────────────────────────────────────────────────

    def get_interest_by_region(self, keyword: str) -> pd.DataFrame:
        """
        Возвращает интерес к запросу по регионам России.
        Полезно для геозависимых запросов.
        """
        try:
            self._build_payload([keyword])
            df = self.pt.interest_by_region(
                resolution="REGION",
                inc_low_vol=True,
                inc_geo_code=False,
            )
            return df.sort_values(keyword, ascending=False)
        except Exception as e:
            logger.warning(f"Interest by region ошибка: {e}")
            return pd.DataFrame()

    # ── Сравнение > 5 ключей ──────────────────────────────────────────────────

    def compare_batch(
        self,
        keywords: List[str],
        timeframe: str = "today 12-m",
        anchor: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Нормализованное сравнение более 5 ключей.

        Метод: каждый батч содержит anchor-ключ (общий якорь).
        Все значения делятся на значение anchor → единая шкала.

        Args:
            keywords: Список ключей (любое кол-во)
            timeframe: Временной период
            anchor:   Якорный ключ (если None — используется keywords[0])
        """
        anchor = anchor or keywords[0]
        batches = [keywords[i:i+4] for i in range(0, len(keywords), 4)]

        frames = []
        anchor_series = None

        for i, batch in enumerate(batches):
            payload = [anchor] + [k for k in batch if k != anchor]
            try:
                df = self.get_interest_over_time(payload, timeframe=timeframe)
                if anchor not in df.columns:
                    continue

                if anchor_series is None:
                    anchor_series = df[anchor].replace(0, 1)
                    frames.append(df)
                else:
                    # Нормализуем через anchor
                    scale = anchor_series / df[anchor].replace(0, 1)
                    for col in df.columns:
                        if col != anchor:
                            df[col] = (df[col] * scale).round(1)
                    frames.append(df[[c for c in df.columns if c != anchor]])

                time.sleep(random.uniform(3, 6))

            except Exception as e:
                logger.warning(f"Батч {i+1} пропущен: {e}")

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, axis=1).fillna(0)

    # ── Внутренние методы ─────────────────────────────────────────────────────

    def _build_payload(
        self,
        keywords: List[str],
        timeframe: str = "today 12-m",
        geo: Optional[str] = None,
        cat: int = 0,
    ):
        """Строит payload с автоповтором при 429."""
        wait = self.backoff
        for attempt in range(self.retries):
            try:
                self.pt.build_payload(
                    keywords[:5],
                    cat=cat,
                    timeframe=timeframe,
                    geo=geo or self.geo,
                )
                return
            except ResponseError as e:
                if "429" in str(e) and attempt < self.retries - 1:
                    logger.warning(f"Rate limit Google Trends, ждём {wait:.0f} сек...")
                    time.sleep(wait)
                    wait *= 2
                else:
                    raise
