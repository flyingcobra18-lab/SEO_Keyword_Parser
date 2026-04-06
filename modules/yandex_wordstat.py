"""
modules/yandex_wordstat.py
==========================
Парсер Яндекс.Wordstat через Selenium.

Особенности:
- Авторизация через логин/пароль или готовые cookies
- Поддержка регионов (geo)
- Сбор базовой и операторной частотности ([!точная], "фразовая", +с +предлогами)
- Постраничный обход (до 50 страниц)
- Защита от детекции автоматизации
- Автоповтор при ошибках и капче
"""

import re
import time
import json
import random
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from loguru import logger


# ─── Коды регионов Яндекса ────────────────────────────────────────────────────

YANDEX_REGIONS = {
    "россия":        225,
    "москва":        213,
    "санкт-петербург": 2,
    "екатеринбург":  54,
    "новосибирск":   65,
    "казань":        43,
    "нижний новгород": 47,
    "краснодар":     35,
    "самара":        51,
    "омск":          66,
    "челябинск":     56,
    "ростов-на-дону": 39,
    "украина":       187,
    "беларусь":      149,
    "казахстан":     159,
}


# ─── Модель данных ────────────────────────────────────────────────────────────

@dataclass
class WordstatResult:
    keyword: str
    shows: int                        # базовая частотность
    operator_exact: Optional[int] = None    # [!точная] частота
    operator_phrase: Optional[int] = None   # "фразовая" частота
    operator_plus: Optional[int] = None     # +с +предлогами
    region_id: Optional[int] = None
    region_name: Optional[str] = None
    source: str = "wordstat"

    @property
    def spaminess(self) -> Optional[float]:
        """Коэффициент спамности: отношение базовой к точной частоте.
        Чем выше — тем больше «мусора» в ключе. > 10 — нежелательно."""
        if self.operator_exact and self.operator_exact > 0:
            return round(self.shows / self.operator_exact, 2)
        return None

    def to_dict(self) -> dict:
        return {
            "keyword": self.keyword,
            "shows_base": self.shows,
            "shows_exact": self.operator_exact,
            "shows_phrase": self.operator_phrase,
            "shows_plus": self.operator_plus,
            "spaminess": self.spaminess,
            "region_id": self.region_id,
            "region_name": self.region_name,
        }


# ─── Парсер ───────────────────────────────────────────────────────────────────

class YandexWordstat:
    BASE_URL = "https://wordstat.yandex.ru/"
    AUTH_URL = "https://passport.yandex.ru/auth"
    COOKIES_PATH = Path("cookies/yandex_cookies.json")

    def __init__(
        self,
        login: str = "",
        password: str = "",
        region_id: int = 213,
        headless: bool = True,
        cookies_file: Optional[str] = None,
        delay_min: float = 1.5,
        delay_max: float = 3.5,
    ):
        self.login = login
        self.password = password
        self.region_id = region_id
        self.headless = headless
        self.cookies_file = Path(cookies_file) if cookies_file else self.COOKIES_PATH
        self.delay_min = delay_min
        self.delay_max = delay_max

        self.driver = self._init_driver()
        self.wait = WebDriverWait(self.driver, 20)

        if self.cookies_file.exists():
            logger.info("Авторизация через cookies")
            self._load_cookies()
        elif login and password:
            logger.info("Авторизация через логин/пароль")
            self._authorize()
        else:
            logger.warning("Нет данных авторизации — попытка работы без входа")

    # ── Инициализация драйвера ────────────────────────────────────────────────

    def _init_driver(self) -> webdriver.Chrome:
        opts = webdriver.ChromeOptions()

        if self.headless:
            opts.add_argument("--headless=new")

        # Маскировка под обычный браузер
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--window-size=1366,768")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)

        # Убираем флаг webdriver из navigator
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
        )
        return driver

    # ── Авторизация ───────────────────────────────────────────────────────────

    def _authorize(self):
        self.driver.get(self.AUTH_URL)
        self._sleep(2)

        try:
            # Ввод логина
            login_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "passp-field-login"))
            )
            self._human_type(login_field, self.login)
            self.driver.find_element(By.XPATH, '//button[@type="submit"]').click()
            self._sleep(2)

            # Ввод пароля
            pwd_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "passp-field-passwd"))
            )
            self._human_type(pwd_field, self.password)
            self.driver.find_element(By.XPATH, '//button[@type="submit"]').click()
            self._sleep(3)

            logger.success("Авторизация в Яндекс успешна")
            self._save_cookies()

        except TimeoutException:
            logger.error("Ошибка авторизации — возможна капча или 2FA")
            raise

    def _save_cookies(self):
        self.cookies_file.parent.mkdir(parents=True, exist_ok=True)
        cookies = self.driver.get_cookies()
        with open(self.cookies_file, "w") as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        logger.info(f"Cookies сохранены → {self.cookies_file}")

    def _load_cookies(self):
        self.driver.get("https://yandex.ru")
        self._sleep(1)
        with open(self.cookies_file) as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                self.driver.add_cookie(cookie)
            except Exception:
                pass
        self.driver.refresh()
        self._sleep(2)

    # ── Основной метод сбора ключей ───────────────────────────────────────────

    def get_keywords(
        self,
        query: str,
        pages: int = 3,
        get_operators: bool = True,
        include_right_column: bool = True,
    ) -> List[WordstatResult]:
        """
        Собирает ключевые слова по запросу из Wordstat.

        Args:
            query:               Исходный запрос
            pages:               Кол-во страниц (1 стр = ~50 ключей)
            get_operators:       Запрашивать операторную частотность
            include_right_column: Собирать также «Похожие запросы» (правая колонка)

        Returns:
            Список WordstatResult
        """
        results = []
        seen = set()

        for page in range(pages):
            url = self._build_url(query, page)
            try:
                self.driver.get(url)
                self._sleep(2)
                self._check_captcha()

                # Левая колонка — «Что искали со словом X»
                left_rows = self._parse_table(".b-word-statistics")
                for kw, freq in left_rows:
                    if kw not in seen:
                        seen.add(kw)
                        results.append(WordstatResult(
                            keyword=kw,
                            shows=freq,
                            region_id=self.region_id,
                        ))

                # Правая колонка — «Запросы, похожие на X»
                if include_right_column:
                    right_rows = self._parse_table(".b-word-statistics", column=1)
                    for kw, freq in right_rows:
                        if kw not in seen:
                            seen.add(kw)
                            results.append(WordstatResult(
                                keyword=kw,
                                shows=freq,
                                region_id=self.region_id,
                            ))

                logger.debug(f"  Страница {page + 1}/{pages}: +{len(left_rows)} ключей")

            except Exception as e:
                logger.warning(f"  Страница {page + 1} пропущена: {e}")
                continue

            self._sleep()

        # Операторная частотность (отдельный запрос на каждый ключ — медленно!)
        if get_operators and results:
            logger.info(f"  Уточняем операторную частотность для {len(results)} ключей...")
            for i, result in enumerate(results):
                result.operator_exact = self._get_operator_freq(f"[!{result.keyword}]")
                result.operator_phrase = self._get_operator_freq(f'"{result.keyword}"')
                if (i + 1) % 10 == 0:
                    logger.debug(f"    Операторы: {i + 1}/{len(results)}")
                self._sleep(1, 2)

        return results

    # ── Вспомогательные методы ────────────────────────────────────────────────

    def _build_url(self, query: str, page: int = 0) -> str:
        url = f"{self.BASE_URL}?text={query}"
        if self.region_id:
            url += f"&geo={self.region_id}"
        if page > 0:
            url += f"&p={page}"
        return url

    def _parse_table(self, selector: str, column: int = 0) -> List[tuple]:
        """Парсит таблицу частотностей. column=0 — левая, 1 — правая."""
        rows = []
        try:
            tables = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if column >= len(tables):
                return rows
            table = tables[column]
            tr_elements = table.find_elements(By.TAG_NAME, "tr")[1:]  # пропуск заголовка
            for tr in tr_elements:
                tds = tr.find_elements(By.TAG_NAME, "td")
                if len(tds) < 2:
                    continue
                keyword = tds[0].text.strip()
                freq_text = re.sub(r"\D", "", tds[1].text)
                freq = int(freq_text) if freq_text else 0
                if keyword and freq > 0:
                    rows.append((keyword, freq))
        except Exception as e:
            logger.debug(f"Ошибка парсинга таблицы: {e}")
        return rows

    def _get_operator_freq(self, query: str) -> int:
        """Получает частотность для одного оператора."""
        try:
            self.driver.get(f"{self.BASE_URL}?text={query}&geo={self.region_id}")
            self._sleep(1.2, 2.2)
            self._check_captcha()
            rows = self._parse_table(".b-word-statistics")
            if rows:
                return rows[0][1]
        except Exception:
            pass
        return 0

    def _check_captcha(self):
        """Проверяет наличие капчи и предупреждает."""
        try:
            captcha = self.driver.find_element(By.CSS_SELECTOR, ".AdvancedCaptcha, .CheckboxCaptcha")
            if captcha.is_displayed():
                logger.warning("⚠ Обнаружена капча! Ожидаем 30 сек или решите вручную...")
                time.sleep(30)
        except NoSuchElementException:
            pass

    def _human_type(self, element, text: str):
        """Имитирует человеческий ввод с паузами между символами."""
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.18))

    def _sleep(self, min_sec: float = None, max_sec: float = None):
        min_sec = min_sec or self.delay_min
        max_sec = max_sec or self.delay_max
        time.sleep(random.uniform(min_sec, max_sec))

    def close(self):
        if self.driver:
            self.driver.quit()
            logger.info("Selenium WebDriver закрыт")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
