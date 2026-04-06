"""
modules/lsi_clustering.py
=========================
Кластеризация ключевых слов для построения структуры сайта.

Два метода:

1. TF-IDF (быстро, без интернета)
   Группирует ключи по символьному сходству (char n-gram TF-IDF).
   Подходит для первичной сортировки большого ядра (1000+ ключей).

2. SERP-метод (точнее, требует данные выдачи)
   Классический метод от SEMrush/Rush Analytics:
   Если два запроса имеют ≥N общих URL в ТОП-10 SERP → они про одно и то же
   и могут продвигаться одной страницей.
   N=3 — рекомендуемый порог.

Дополнительно:
- Определение главного ключа кластера (наиболее частотный)
- Оценка интента кластера (информационный / транзакционный / навигационный)
- Удаление дубликатов и слишком похожих ключей
"""

import re
from collections import Counter
from typing import List, Dict, Optional, Tuple

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
from loguru import logger


# ─── Определение интента ──────────────────────────────────────────────────────

INTENT_PATTERNS = {
    "транзакционный": [
        r"купить", r"заказать", r"цена", r"стоимость", r"дёшево", r"дешево",
        r"скидк", r"акц", r"распродаж", r"интернет.магазин", r"оптом",
        r"доставк", r"магазин", r"shop", r"buy", r"price",
    ],
    "информационный": [
        r"что такое", r"как ", r"зачем", r"почему", r"когда", r"где",
        r"отзыв", r"обзор", r"сравнен", r"рейтинг", r"лучш", r"топ",
        r"инструкц", r"руководств", r"советы", r"способы", r"виды",
    ],
    "навигационный": [
        r"сайт", r"официальн", r"войти", r"личный кабинет", r"логин",
        r"скачать", r"download", r"ru$", r"\.com",
    ],
    "коммерческий": [
        r"аренда", r"прокат", r"услуг", r"заявка", r"консультац",
        r"договор", r"оформить", r"получить", r"подобрать",
    ],
}


def detect_intent(keywords: List[str]) -> str:
    """Определяет преобладающий интент кластера."""
    scores = Counter()
    combined = " ".join(keywords).lower()

    for intent, patterns in INTENT_PATTERNS.items():
        for p in patterns:
            if re.search(p, combined):
                scores[intent] += 1

    if not scores:
        return "смешанный"
    return scores.most_common(1)[0][0]


# ─── Основной класс ───────────────────────────────────────────────────────────

class LSIClusterer:
    """
    Кластеризатор ключевых слов.

    Args:
        n_clusters:      Желаемое кол-во кластеров (для TF-IDF)
        serp_threshold:  Мин. пересечений URL для SERP-метода (3 = стандарт)
        similarity_threshold: Порог схожести для дедупликации (0.95 = строго)
    """

    def __init__(
        self,
        n_clusters: int = 15,
        serp_threshold: int = 3,
        similarity_threshold: float = 0.92,
    ):
        self.n_clusters = n_clusters
        self.serp_threshold = serp_threshold
        self.similarity_threshold = similarity_threshold

    # ── TF-IDF кластеризация ─────────────────────────────────────────────────

    def cluster_by_tfidf(
        self, keywords: List[str], freq_map: Optional[Dict[str, int]] = None
    ) -> Dict[str, dict]:
        """
        Кластеризация по символьным n-gram TF-IDF.

        Args:
            keywords:  Список ключей
            freq_map:  {keyword: частотность} — для выбора главного ключа

        Returns:
            {cluster_name: {
                "keywords": [...],
                "main_keyword": "...",
                "intent": "...",
                "size": N,
            }}
        """
        if len(keywords) < 2:
            return {keywords[0]: {"keywords": keywords, "main_keyword": keywords[0],
                                   "intent": "смешанный", "size": 1}}

        n = min(self.n_clusters, len(keywords))

        # char_wb — символьные n-gram с учётом границ слов
        vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 5), min_df=1)
        X = vec.fit_transform(keywords)
        sim_matrix = cosine_similarity(X)
        distance_matrix = 1 - sim_matrix
        np.fill_diagonal(distance_matrix, 0)
        distance_matrix = np.clip(distance_matrix, 0, None)

        model = AgglomerativeClustering(
            n_clusters=n,
            metric="precomputed",
            linkage="complete",
        )
        labels = model.fit_predict(distance_matrix)

        return self._build_clusters(keywords, labels.tolist(), freq_map)

    # ── SERP-кластеризация ────────────────────────────────────────────────────

    def cluster_by_serp(
        self,
        keywords: List[str],
        serp_data: Dict[str, List[str]],
        freq_map: Optional[Dict[str, int]] = None,
    ) -> Dict[str, dict]:
        """
        Кластеризация по пересечению SERP.

        Args:
            keywords:  Ключи для кластеризации
            serp_data: {keyword: [url1, url2, ...url10]}
            freq_map:  Частотности для выбора главного ключа

        Returns:
            {cluster_name: {keywords, main_keyword, intent, size, serp_overlap}}

        Алгоритм (метод Rush Analytics):
        1. Берём ключ K1, смотрим его ТОП-10 URL
        2. Для каждого K2 считаем кол-во совпадающих URL с K1
        3. Если совпадений >= serp_threshold → K1 и K2 в одном кластере
        4. Повторяем для несгруппированных ключей
        """
        clusters = {}
        used = set()

        # Сортируем по частотности (самые частотные — главные кластеры)
        if freq_map:
            keywords = sorted(keywords, key=lambda k: freq_map.get(k, 0), reverse=True)

        for kw1 in keywords:
            if kw1 in used:
                continue

            group = [kw1]
            urls1 = set(serp_data.get(kw1, []))

            for kw2 in keywords:
                if kw2 == kw1 or kw2 in used:
                    continue
                urls2 = set(serp_data.get(kw2, []))
                overlap = len(urls1 & urls2)
                if overlap >= self.serp_threshold:
                    group.append(kw2)
                    used.add(kw2)

            used.add(kw1)

            main_kw = self._pick_main_keyword(group, freq_map)
            clusters[main_kw] = {
                "keywords":    group,
                "main_keyword": main_kw,
                "intent":      detect_intent(group),
                "size":        len(group),
                "serp_overlap": self.serp_threshold,
            }

        return clusters

    # ── Дедупликация ──────────────────────────────────────────────────────────

    def deduplicate(
        self, keywords: List[str], threshold: Optional[float] = None
    ) -> List[str]:
        """
        Удаляет слишком похожие ключи (почти-дубликаты).
        Например: "купить ноутбук" и "купить ноутбуки" при threshold=0.95

        Returns:
            Список уникальных ключей
        """
        threshold = threshold or self.similarity_threshold
        if len(keywords) < 2:
            return keywords

        vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 5))
        X = vec.fit_transform(keywords)
        sim = cosine_similarity(X)

        keep = []
        removed = set()

        for i, kw in enumerate(keywords):
            if i in removed:
                continue
            keep.append(kw)
            for j in range(i + 1, len(keywords)):
                if sim[i][j] >= threshold:
                    removed.add(j)

        logger.debug(f"Дедупликация: {len(keywords)} → {len(keep)} ключей")
        return keep

    # ── Группировка по первому слову ──────────────────────────────────────────

    def cluster_by_first_word(self, keywords: List[str]) -> Dict[str, List[str]]:
        """
        Простая группировка по первому слову запроса.
        Быстро, полезно для первичного обзора ядра.
        """
        groups: Dict[str, List[str]] = {}
        for kw in keywords:
            first = kw.strip().split()[0] if kw.strip() else "прочее"
            groups.setdefault(first, []).append(kw)
        return dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True))

    # ── Внутренние методы ─────────────────────────────────────────────────────

    def _build_clusters(
        self,
        keywords: List[str],
        labels: List[int],
        freq_map: Optional[Dict[str, int]],
    ) -> Dict[str, dict]:
        raw: Dict[int, List[str]] = {}
        for idx, label in enumerate(labels):
            raw.setdefault(label, []).append(keywords[idx])

        result = {}
        for label, kws in sorted(raw.items(), key=lambda x: len(x[1]), reverse=True):
            main_kw = self._pick_main_keyword(kws, freq_map)
            result[main_kw] = {
                "keywords":     kws,
                "main_keyword": main_kw,
                "intent":       detect_intent(kws),
                "size":         len(kws),
            }
        return result

    def _pick_main_keyword(
        self, keywords: List[str], freq_map: Optional[Dict[str, int]]
    ) -> str:
        """Выбирает главный ключ кластера: самый частотный или самый короткий."""
        if freq_map:
            return max(keywords, key=lambda k: freq_map.get(k, 0))
        # Без частотности — выбираем самый короткий (обычно самый общий)
        return min(keywords, key=lambda k: len(k.split()))

    # ── Статистика кластеров ──────────────────────────────────────────────────

    def cluster_stats(self, clusters: Dict[str, dict]) -> dict:
        """Возвращает сводную статистику по кластерам."""
        sizes = [v["size"] for v in clusters.values()]
        intents = Counter(v["intent"] for v in clusters.values())
        return {
            "total_clusters": len(clusters),
            "total_keywords": sum(sizes),
            "avg_cluster_size": round(np.mean(sizes), 1),
            "max_cluster_size": max(sizes),
            "min_cluster_size": min(sizes),
            "intent_distribution": dict(intents),
        }
