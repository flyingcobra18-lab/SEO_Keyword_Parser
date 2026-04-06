"""
modules/competitor_parser.py
============================
Парсер конкурентов из ТОП SERP Яндекс и Google.

Что делает:
- Извлекает URL из ТОП-N выдачи
- Парсит Title, H1-H3, мета-теги, структуру страницы конкурентов
- Считает плотность ключевых слов (TF)
- Определяет технические характеристики: скорость, CMS, разметку Schema.org
- Собирает ключи из alt-атрибутов изображений
- Анализирует внутреннюю перелинковку (якорные тексты ссылок)

Примечание: прямой парсинг Google затруднён — рекомендуется Яндекс или
использование официального API (Google Custom Search JSON API).
"""

import re
import time
import random
from typing import List, Dict, Optional
from urllib.parse import urlparse, urljoin, quote_plus
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup
from loguru import logger


# ─── Модели данных ────────────────────────────────────────────────────────────

@dataclass
class CompetitorPage:
    url: str
    title: str = ""
    h1: List[str] = field(default_factory=list)
    h2: List[str] = field(default_factory=list)
    h3: List[str] = field(default_factory=list)
    meta_description: str = ""
    meta_keywords: str = ""
    word_count: int = 0
    images_count: int = 0
    img_alts: List[str] = field(default_factory=list)
    internal_anchors: List[str] = field(default_factory=list)
    schema_types: List[str] = field(default_factory=list)
    has_faq: bool = False
    has_breadcrumbs: bool = False
    cms: str = ""
    load_time_ms: Optional[int] = None
    domain: str = ""
    is_accessible: bool = True

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "domain": self.domain,
            "title": self.title,
            "h1": " | ".join(self.h1),
            "h2_count": len(self.h2),
            "h3_count": len(self.h3),
            "meta_description": self.meta_description,
            "meta_keywords": self.meta_keywords,
            "word_count": self.word_count,
            "images_count": self.images_count,
            "schema_types": ", ".join(self.schema_types),
            "has_faq": self.has_faq,
            "has_breadcrumbs": self.has_breadcrumbs,
            "cms": self.cms,
            "load_time_ms": self.load_time_ms,
        }

    def all_text_elements(self) -> List[str]:
        """Все текстовые SEO-элементы для анализа ключей."""
        return [self.title, *self.h1, *self.h2, *self.h3,
                self.meta_description, self.meta_keywords, *self.img_alts]


# ─── Парсер ───────────────────────────────────────────────────────────────────

class CompetitorParser:
    """
    Парсер конкурентов из поисковой выдачи.

    Args:
        timeout:     Таймаут HTTP-запросов (сек)
        delay_min:   Мин. пауза между запросами
        delay_max:   Макс. пауза между запросами
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    # Признаки CMS по meta/body
    CMS_SIGNATURES = {
        "WordPress":  [r"wp-content", r"wp-includes", r"wordpress"],
        "Bitrix":     [r"bitrix", r"bx-"],
        "1С-Битрикс": [r"/bitrix/", r"BX\."],
        "Tilda":      [r"tildacdn", r"tilda"],
        "Drupal":     [r"drupal", r"/sites/default/files"],
        "Joomla":     [r"joomla", r"/components/com_"],
        "OpenCart":   [r"opencart", r"route=product"],
        "Magento":    [r"magento", r"Mage\."],
        "ModX":       [r"modx", r"[["],
        "Wix":        [r"wixsite", r"wix.com"],
    }

    def __init__(
        self,
        timeout: int = 10,
        delay_min: float = 1.0,
        delay_max: float = 2.5,
    ):
        self.timeout = timeout
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    # ── Получение URL из SERP ─────────────────────────────────────────────────

    def get_serp_urls(
        self,
        query: str,
        engine: str = "yandex",
        top_n: int = 10,
        region: str = "213",
    ) -> List[str]:
        """
        Парсит URL из ТОП поисковой выдачи.

        Args:
            query:  Запрос
            engine: 'yandex' или 'google'
            top_n:  Кол-во результатов
            region: Регион Яндекса (213 = Москва)

        Returns:
            Список URL без рекламы и агрегаторов
        """
        if engine == "yandex":
            return self._get_yandex_serp(query, top_n, region)
        elif engine == "google":
            return self._get_google_serp(query, top_n)
        else:
            raise ValueError(f"Неизвестный движок: {engine}")

    def _get_yandex_serp(self, query: str, top_n: int, region: str) -> List[str]:
        url = f"https://yandex.ru/search/?text={quote_plus(query)}&numdoc={top_n}&lr={region}"
        try:
            start = time.time()
            r = self.session.get(url, timeout=self.timeout)
            soup = BeautifulSoup(r.text, "lxml")

            urls = []
            # Органические результаты (исключаем рекламу)
            for item in soup.select(".serp-item"):
                if item.get("data-fast-name") in ("adv", "direct"):
                    continue
                link = item.select_one(".organic__url, .serp-item__url, a[href]")
                if link:
                    href = link.get("href", "")
                    if href.startswith("http") and urlparse(href).netloc:
                        urls.append(href)

            logger.debug(f"Яндекс SERP «{query}»: {len(urls)} URL")
            return self._filter_urls(urls)[:top_n]

        except Exception as e:
            logger.warning(f"Ошибка парсинга Яндекс SERP: {e}")
            return []

    def _get_google_serp(self, query: str, top_n: int) -> List[str]:
        """
        Парсинг Google нестабилен — рекомендуется Custom Search API.
        Этот метод работает, но может блокироваться.
        """
        url = f"https://www.google.ru/search?q={quote_plus(query)}&num={top_n}&hl=ru"
        try:
            r = self.session.get(url, timeout=self.timeout)
            soup = BeautifulSoup(r.text, "lxml")
            urls = []
            for a in soup.select("div.yuRUbf > a, div.tF2Cxc a"):
                href = a.get("href", "")
                if href.startswith("http"):
                    urls.append(href)
            return self._filter_urls(urls)[:top_n]
        except Exception as e:
            logger.warning(f"Ошибка парсинга Google SERP: {e}")
            return []

    # ── Парсинг страниц конкурентов ───────────────────────────────────────────

    def extract_page_keywords(self, url: str) -> Optional[CompetitorPage]:
        """
        Извлекает SEO-данные со страницы конкурента.

        Возвращает CompetitorPage или None при ошибке.
        """
        page = CompetitorPage(url=url, domain=urlparse(url).netloc)
        try:
            start = time.time()
            r = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            page.load_time_ms = int((time.time() - start) * 1000)

            if r.status_code != 200:
                logger.debug(f"HTTP {r.status_code}: {url}")
                page.is_accessible = False
                return page

            soup = BeautifulSoup(r.text, "lxml")

            # Title
            page.title = soup.title.get_text(strip=True) if soup.title else ""

            # H1–H3
            page.h1 = [h.get_text(strip=True) for h in soup.find_all("h1")]
            page.h2 = [h.get_text(strip=True) for h in soup.find_all("h2")]
            page.h3 = [h.get_text(strip=True) for h in soup.find_all("h3")]

            # Meta-теги
            desc_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
            page.meta_description = desc_tag.get("content", "") if desc_tag else ""

            kw_tag = soup.find("meta", attrs={"name": re.compile(r"^keywords$", re.I)})
            page.meta_keywords = kw_tag.get("content", "") if kw_tag else ""

            # Количество слов (очищаем от script/style)
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = soup.get_text(separator=" ")
            page.word_count = len(text.split())

            # Изображения
            imgs = soup.find_all("img")
            page.images_count = len(imgs)
            page.img_alts = [
                img.get("alt", "").strip()
                for img in imgs if img.get("alt", "").strip()
            ]

            # Якорные тексты внутренних ссылок
            base = urlparse(url).netloc
            page.internal_anchors = [
                a.get_text(strip=True)
                for a in soup.find_all("a", href=True)
                if urlparse(a["href"]).netloc in ("", base)
                and a.get_text(strip=True)
            ]

            # Schema.org разметка
            page.schema_types = self._extract_schema_types(soup)

            # FAQ
            page.has_faq = bool(
                soup.find(attrs={"itemtype": re.compile(r"FAQPage", re.I)}) or
                soup.find(attrs={"@type": "FAQPage"}) or
                soup.find(class_=re.compile(r"faq", re.I))
            )

            # Хлебные крошки
            page.has_breadcrumbs = bool(
                soup.find(attrs={"itemtype": re.compile(r"BreadcrumbList", re.I)}) or
                soup.find(class_=re.compile(r"breadcrumb", re.I)) or
                soup.find(attrs={"aria-label": re.compile(r"breadcrumb", re.I)})
            )

            # CMS
            page.cms = self._detect_cms(r.text)

            logger.debug(
                f"  ✓ {page.domain} | {page.word_count} слов | "
                f"{page.images_count} img | {page.load_time_ms}мс"
            )

        except requests.exceptions.Timeout:
            logger.debug(f"Таймаут: {url}")
            page.is_accessible = False
        except Exception as e:
            logger.debug(f"Ошибка парсинга {url}: {e}")
            page.is_accessible = False

        self._sleep()
        return page

    def analyze_competitors(
        self, query: str, engine: str = "yandex", top_n: int = 10
    ) -> Dict:
        """
        Полный анализ конкурентов для одного запроса.

        Возвращает сводную статистику + данные по каждой странице.
        """
        urls = self.get_serp_urls(query, engine, top_n)
        pages = []
        for url in urls:
            page = self.extract_page_keywords(url)
            if page and page.is_accessible:
                pages.append(page)

        if not pages:
            return {"query": query, "pages": [], "summary": {}}

        # Сводная статистика
        word_counts = [p.word_count for p in pages]
        summary = {
            "query":             query,
            "total_analyzed":    len(pages),
            "avg_word_count":    int(sum(word_counts) / len(word_counts)),
            "max_word_count":    max(word_counts),
            "min_word_count":    min(word_counts),
            "faq_count":         sum(1 for p in pages if p.has_faq),
            "breadcrumb_count":  sum(1 for p in pages if p.has_breadcrumbs),
            "schema_count":      sum(1 for p in pages if p.schema_types),
            "cms_distribution":  self._count_cms(pages),
            "common_h2":         self._find_common_headings(pages),
        }

        return {
            "query":   query,
            "urls":    urls,
            "pages":   [p.to_dict() for p in pages],
            "summary": summary,
        }

    # ── Вспомогательные методы ────────────────────────────────────────────────

    def _extract_schema_types(self, soup: BeautifulSoup) -> List[str]:
        """Извлекает типы Schema.org разметки со страницы."""
        types = set()

        # Microdata
        for el in soup.find_all(attrs={"itemtype": True}):
            match = re.search(r"schema\.org/(\w+)", el["itemtype"])
            if match:
                types.add(match.group(1))

        # JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "{}")
                if isinstance(data, dict) and "@type" in data:
                    types.add(data["@type"])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "@type" in item:
                            types.add(item["@type"])
            except Exception:
                pass

        return list(types)

    def _detect_cms(self, html: str) -> str:
        """Определяет CMS по HTML-коду страницы."""
        html_lower = html.lower()
        for cms, patterns in self.CMS_SIGNATURES.items():
            if any(re.search(p, html_lower) for p in patterns):
                return cms
        return "неизвестно"

    def _count_cms(self, pages: List[CompetitorPage]) -> Dict[str, int]:
        counter: Dict[str, int] = {}
        for p in pages:
            counter[p.cms] = counter.get(p.cms, 0) + 1
        return dict(sorted(counter.items(), key=lambda x: x[1], reverse=True))

    def _find_common_headings(self, pages: List[CompetitorPage]) -> List[str]:
        """Находит H2-заголовки, которые встречаются у 2+ конкурентов."""
        from collections import Counter

        all_h2 = []
        for p in pages:
            all_h2.extend([h.lower().strip() for h in p.h2])

        counts = Counter(all_h2)
        return [h for h, cnt in counts.most_common(20) if cnt >= 2]

    def _filter_urls(self, urls: List[str]) -> List[str]:
        """Убирает агрегаторы, соцсети и нерелевантные домены."""
        SKIP_DOMAINS = {
            "youtube.com", "youtu.be", "vk.com", "ok.ru",
            "wikipedia.org", "wikimedia.org",
            "yandex.ru", "google.ru", "google.com",
            "twitter.com", "instagram.com", "tiktok.com",
            "avito.ru", "ozon.ru", "wildberries.ru", "market.yandex.ru",
        }
        clean = []
        seen = set()
        for url in urls:
            domain = urlparse(url).netloc.lstrip("www.")
            if domain not in SKIP_DOMAINS and domain not in seen:
                clean.append(url)
                seen.add(domain)
        return clean

    def _sleep(self):
        time.sleep(random.uniform(self.delay_min, self.delay_max))
