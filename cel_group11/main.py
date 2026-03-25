import asyncio
import logging
from typing import Dict, List
import os
import shutil
from datetime import datetime
from collections import defaultdict

from .scraper import BoutiqaatCelebrityScraper
from .s3_uploader import S3Uploader
from .excel_generator import ExcelGenerator
from config import TEMP_DIR, S3_EXCEL_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Hardcoded celebrity URLs for group 51 (URLs 1251–1275)
CELEBRITY_URLS = [
    "https://www.boutiqaat.com/ar-kw/women/lady-december-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/lady-roosha-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/lady-rain-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/lady-aysha-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/leesa-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/levelkw-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/lelas-nama-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/laila-alrandie-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/laila-almeqbali-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/laila-alyafie/cb/",
    "https://www.boutiqaat.com/ar-kw/women/layla-shehab-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/laila-abdallah-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/laila-abdull4h-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/laila-mourad-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/lilia-atrash-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/liliane-zaher-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/leen-alsharif-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/leen-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/lina-abyadh-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/linda-mohammed-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/engsarah-boutique/cb/",
    "https://www.boutiqaat.com/ar-kw/women/mothers-goals-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/mar-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/maruko-boutique-1/cb/",
    "https://www.boutiqaat.com/ar-kw/women/maria-alfaraj/cb/",
]


class BoutiqaatCelebrityPipeline:
    """Scrape, process and upload products for a batch of celebrities."""

    def __init__(self):
        self.uploader = S3Uploader()
        self.excel_generator = ExcelGenerator()

    # ------------------------------------------------------------------
    # Async layer
    # ------------------------------------------------------------------

    async def _process_celebrity_async(
        self, semaphore: asyncio.Semaphore, url: str
    ) -> bool:
        """Acquire semaphore slot and process one celebrity URL in a thread."""
        async with semaphore:
            slug = url.rstrip("/").split("/")[-2]
            logger.info(f"[Slot acquired] Starting celebrity: {slug}")
            scraper = BoutiqaatCelebrityScraper()
            try:
                return await asyncio.to_thread(self._process_celebrity, scraper, url)
            except Exception as exc:
                logger.error(f"Error processing {slug}: {exc}")
                return False

    def run(self) -> bool:
        logger.info("=" * 80)
        logger.info("Starting Celebrity Pipeline (Async – Semaphore=3)")
        logger.info("=" * 80)
        try:
            if not self.uploader.test_connection():
                logger.error("S3 connection failed. Exiting.")
                return False

            logger.info(
                f"Processing {len(CELEBRITY_URLS)} celebrities (max 3 concurrent)"
            )
            semaphore = asyncio.Semaphore(3)

            async def _gather_all():
                return await asyncio.gather(
                    *[
                        self._process_celebrity_async(semaphore, url)
                        for url in CELEBRITY_URLS
                    ],
                    return_exceptions=True,
                )

            results = asyncio.run(_gather_all())

            successful = sum(1 for r in results if r is True)
            failed = len(results) - successful
            logger.info("=" * 80)
            logger.info(f"Pipeline Complete: {successful} successful, {failed} failed")
            logger.info("=" * 80)
            return True
        except Exception as exc:
            logger.error(f"Pipeline failed: {exc}")
            return False
        finally:
            import shutil as _shutil
            if os.path.exists(TEMP_DIR):
                try:
                    _shutil.rmtree(TEMP_DIR)
                    logger.info("Cleaned up temporary files")
                except Exception as exc:
                    logger.warning(f"Failed to cleanup temp files: {exc}")

    # ------------------------------------------------------------------
    # Core processing (runs inside asyncio.to_thread)
    # ------------------------------------------------------------------

    def _process_celebrity(
        self, scraper: BoutiqaatCelebrityScraper, celebrity_url: str
    ) -> bool:
        slug = celebrity_url.rstrip("/").split("/")[-2]
        celebrity_name = slug.replace("-", " ").title()

        try:
            products = scraper.get_celebrity_products(celebrity_url)
            if not products:
                logger.info(f"Skipping {celebrity_name} – no products available")
                return True

            logger.info(
                f"Found {len(products)} products for celebrity: {celebrity_name}"
            )

            for idx, product in enumerate(products, 1):
                logger.info(
                    f"  [{idx}/{len(products)}] Processing: {product.get('name', 'Unknown')}"
                )
                try:
                    full = scraper.get_product_full_details(product["url"])
                    if full:
                        product.update(full)
                    if product.get("image_url"):
                        product["s3_image_path"] = self._upload_product_image(
                            product, celebrity_name
                        )
                    else:
                        product["s3_image_path"] = "No image available"
                except Exception as exc:
                    logger.warning(f"    Error processing product: {exc}")
                    continue

            subcategories_data = defaultdict(list)
            for product in products:
                key = product.get("subcategory", celebrity_name)
                subcategories_data[key].append(product)

            excel_file = self.excel_generator.create_category_workbook(
                celebrity_name, subcategories_data
            )
            self._upload_excel_file(excel_file, celebrity_name)

            logger.info(f"✓ Completed celebrity: {celebrity_name}")
            return True
        except Exception as exc:
            logger.error(f"✗ Failed celebrity {celebrity_name}: {exc}")
            return False

    # ------------------------------------------------------------------
    # S3 helpers
    # ------------------------------------------------------------------

    def _upload_product_image(self, product: Dict, celebrity_name: str) -> str:
        try:
            image_url = product.get("image_url")
            sku = product.get("sku", "unknown")
            if not image_url:
                return "No image URL"
            safe = (
                "".join(c for c in celebrity_name if c.isalnum() or c in " _-")
                .rstrip()
                .replace(" ", "_")
            )
            s3_path = (
                f"boutiqaat-data/year={datetime.now().strftime('%Y')}/"
                f"month={datetime.now().strftime('%m')}/"
                f"day={datetime.now().strftime('%d')}/celebrities/images/{safe}"
            )
            s3_key = self.uploader.upload_image_from_url(image_url, f"{sku}_image.jpg", s3_path)
            return s3_key if s3_key else "Upload failed"
        except Exception as exc:
            logger.warning(f"Error uploading image for {product.get('name')}: {exc}")
            return "Error"

    def _upload_excel_file(self, local_path: str, celebrity_name: str) -> bool:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe = (
                "".join(c for c in celebrity_name if c.isalnum() or c in " _-")
                .rstrip()
                .replace(" ", "_")
            )
            s3_path = (
                f"boutiqaat-data/year={datetime.now().strftime('%Y')}/"
                f"month={datetime.now().strftime('%m')}/"
                f"day={datetime.now().strftime('%d')}/celebrities/excel-files"
            )
            s3_key = self.uploader.upload_local_file(
                local_path, s3_path, f"{safe}_{timestamp}.xlsx"
            )
            if s3_key:
                logger.info(f"Excel uploaded: {s3_key}")
                return True
            logger.error(f"Failed to upload Excel: {local_path}")
            return False
        except Exception as exc:
            logger.error(f"Error uploading Excel: {exc}")
            return False


if __name__ == "__main__":
    pipeline = BoutiqaatCelebrityPipeline()
    success = pipeline.run()
    exit(0 if success else 1)
