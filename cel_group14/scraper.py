import logging
import re
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright  # noqa: F401
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BoutiqaatCelebrityScraper:
    """Scrapes products from a single celebrity /cb/ page."""

    def __init__(self):
        self.base_url = "https://www.boutiqaat.com"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _clean_url(self, url: str) -> str:
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = urljoin(self.base_url, url)
        url = re.sub(r"(?<!:)//+", "/", url)
        return url

    def _extract_image_url(self, elem) -> str:
        img = elem.select_one("img.img-fluid")
        if img:
            raw = img.get("data-src") or img.get("src", "")
            if raw:
                return self._clean_url(raw)
        return ""

    def _make_request_with_js(
        self, url: str, is_celebrity_page: bool = False
    ) -> Optional[BeautifulSoup]:
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                logger.info(f"Loading page: {url}")
                page.goto(url, wait_until="domcontentloaded", timeout=90_000)

                if is_celebrity_page or "/cb/" in url:
                    try:
                        page.wait_for_selector("div.single-product-wrap", timeout=30_000)
                        has_products = True
                    except Exception:
                        logger.warning(f"No products on page: {url}")
                        has_products = False

                    if has_products:
                        time.sleep(6)
                        logger.info("Infinite scroll starting…")
                        no_change = 0
                        for attempt in range(100):
                            before = page.evaluate(
                                "document.querySelectorAll('div.single-product-wrap').length"
                            )
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            time.sleep(6)
                            try:
                                page.wait_for_load_state("networkidle", timeout=8_000)
                            except Exception:
                                time.sleep(3)
                            after = page.evaluate(
                                "document.querySelectorAll('div.single-product-wrap').length"
                            )
                            logger.info(
                                f"Scroll {attempt + 1}: {before} → {after} products"
                            )
                            if after == before:
                                no_change += 1
                                if no_change >= 5:
                                    logger.info(
                                        f"No new products after 5 attempts. Stopping at {after}."
                                    )
                                    break
                            else:
                                no_change = 0
                        total = page.evaluate(
                            "document.querySelectorAll('div.single-product-wrap').length"
                        )
                        logger.info(f"Scroll complete. Total products: {total}")
                elif "/p/" in url:
                    try:
                        page.wait_for_load_state("networkidle", timeout=10_000)
                    except Exception:
                        time.sleep(3)
                else:
                    time.sleep(3)
                    try:
                        page.wait_for_load_state("networkidle", timeout=10_000)
                    except Exception:
                        time.sleep(2)

                html = page.content()
                browser.close()
                return BeautifulSoup(html, "html.parser")
        except Exception as exc:
            logger.error(f"Browser request failed for {url}: {exc}")
            return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_celebrity_products(self, celebrity_url: str) -> List[Dict]:
        """Return all products scraped from a celebrity page."""
        logger.info(f"Fetching products from: {celebrity_url}")
        soup = self._make_request_with_js(celebrity_url, is_celebrity_page=True)
        if not soup:
            logger.error("Failed to load celebrity page")
            return []
        products = self._extract_all_products(soup, celebrity_url)
        if not products:
            logger.info("Celebrity page loaded but contains no products")
        return products

    def _extract_all_products(self, soup: BeautifulSoup, source_url: str) -> List[Dict]:
        products = []
        elems = soup.select("div.single-product-wrap")
        logger.info(f"Found {len(elems)} product elements")
        for elem in elems:
            try:
                product = self._extract_product_details(elem, source_url)
                if product:
                    products.append(product)
            except Exception as exc:
                logger.warning(f"Error extracting product: {exc}")
        logger.info(f"Extracted {len(products)} products")
        return products

    def _extract_product_details(self, elem, source_url: str) -> Optional[Dict]:
        try:
            product: Dict = {}

            link = elem.find("a", href=lambda x: x and "/p/" in str(x))
            if not link or not link.get("href"):
                return None
            product["url"] = self._clean_url(link["href"])
            product["product_url"] = product["url"]

            # Name
            name_elem = (
                elem.select_one("h4.product-name a")
                or elem.select_one("h4.product-name")
                or elem.select_one("h4 a")
                or elem.select_one("h4")
                or elem.select_one("a.product-name")
                or elem.select_one("div.product-name a")
                or elem.select_one("div.product-name")
                or elem.select_one("div.product-title a")
                or elem.select_one("div.product-title")
            )
            if name_elem:
                product["name"] = name_elem.get_text(strip=True)
            elif link.get("title"):
                product["name"] = link["title"].strip()
            elif link.get("aria-label"):
                product["name"] = link["aria-label"].strip()
            else:
                parts = product["url"].split("/")
                for part in parts:
                    if (
                        part
                        and part not in ("ar-kw", "women", "p", "cid", "ruleId")
                        and not part.isdigit()
                    ):
                        product["name"] = part.replace("-", " ").title()
                        break
                else:
                    product["name"] = "Unknown"

            # Brand
            brand_elem = (
                elem.select_one("h5.brand-title a")
                or elem.select_one("h5.brand-title")
                or elem.select_one("h5 a")
                or elem.select_one("h5")
                or elem.select_one("div.brand-title a")
                or elem.select_one("div.brand-title")
                or elem.select_one("span.brand")
            )
            product["brand"] = brand_elem.get_text(strip=True) if brand_elem else ""

            # Price
            price_elem = elem.select_one("span.price-sale") or elem.select_one("span.price")
            product["price"] = price_elem.get_text(strip=True) if price_elem else ""

            # Original price
            orig = elem.select_one("span.price-regular")
            product["original_price"] = orig.get_text(strip=True) if orig else ""

            # Image
            product["image_url"] = self._extract_image_url(elem)

            # Rating
            rating_elem = elem.select_one("div.product-rating")
            if rating_elem:
                product["rating"] = str(len(rating_elem.select("i.fa-star")))
            else:
                product["rating"] = ""

            # SKU from URL
            url_path = product["url"].split("/p/")[0]
            parts = url_path.rstrip("/").split("/")
            if parts:
                potential = parts[-1]
                m = re.search(
                    r"([A-Z]+-\d+-\d+|[A-Z]{2,}-\d{5,})$", potential, re.IGNORECASE
                )
                product["sku"] = m.group(1).upper() if m else potential
            else:
                product["sku"] = ""

            product["subcategory"] = "celebrity-product"
            return product
        except Exception as exc:
            logger.warning(f"Error in _extract_product_details: {exc}")
            return None

    def get_product_full_details(self, product_url: str) -> Optional[Dict]:
        """Fetch additional details from a product detail page."""
        logger.debug(f"Fetching full details: {product_url}")
        soup = self._make_request_with_js(product_url, is_celebrity_page=False)
        if not soup:
            return None
        try:
            details: Dict = {}
            desc = soup.select_one("div.description-content") or soup.select_one(
                "div.product-description"
            )
            details["description"] = desc.get_text(strip=True)[:500] if desc else ""
            discount = soup.select_one("span.discount-percentage") or soup.select_one(
                "div.discount-badge"
            )
            details["discount"] = discount.get_text(strip=True) if discount else ""
            stock = soup.select_one("div.stock-status") or soup.select_one("span.in-stock")
            details["stock_status"] = stock.get_text(strip=True) if stock else "Unknown"
            reviews = soup.select_one("span.reviews-count")
            details["reviews_count"] = reviews.get_text(strip=True) if reviews else "0"
            return details
        except Exception as exc:
            logger.warning(f"Error extracting full details: {exc}")
            return None
