#!/usr/bin/env python3

import asyncio
import aiohttp
import argparse
import csv
import hashlib
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, quote
from xml.etree import ElementTree as ET

try:
    from bs4 import BeautifulSoup
    from tqdm import tqdm
    import spacy
except ImportError as e:
    print(f"Error: missing dependencies")
    print(f"  Linux/Mac: ./setup.sh")
    print(f"  Windows:   setup.bat")
    sys.exit(1)

BASE_URL = "https://www.archivportal-d.de"
SEARCH_URL = f"{BASE_URL}/objekte"
QUERY = "Bürgerinitiativen"
ROWS_PER_PAGE = 100
MAX_CONCURRENT = 30
TIMEOUT = 30



@dataclass
class Initiative:
    titre: str
    periode: str
    lieu: str
    url: str = ""
    institution: str = ""

    def to_dict(self):
        return asdict(self)

    def hash_key(self) -> str:
        match = re.search(r'/item/([A-Z0-9]+)', self.url)
        if match:
            return match.group(1)
        key = f"{self.titre.lower().strip()}|{self.periode}|{self.lieu.lower().strip()}"
        return hashlib.md5(key.encode()).hexdigest()


class ArchivportalScraper:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.results: list[Initiative] = []
        self.seen_hashes: set[str] = set()
        self.errors: list[dict] = []
        self.duplicates: list[dict] = []
        self.parse_failures: list[dict] = []
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.nlp = spacy.load('de_core_news_md')

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; ArchivScraper/1.0; educational research)',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def fetch(self, url: str, retries: int = 3) -> Optional[str]:
        async with self.semaphore:
            for attempt in range(retries):
                try:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:
                            wait = 2 ** attempt
                            await asyncio.sleep(wait)
                        else:
                            self.errors.append({'url': url, 'status': response.status})
                            return None
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)
                except Exception as e:
                    if attempt == retries - 1:
                        self.errors.append({'url': url, 'error': str(e)})
            return None

    async def get_total_results(self) -> int:
        url = f"{SEARCH_URL}?lang=en&query={quote(QUERY)}&offset=0&rows=1"
        html = await self.fetch(url)
        if not html:
            return 0

        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        match = re.search(r'of\s+([\d,]+)', text)
        if match:
            return int(match.group(1).replace(',', ''))
        return 0

    def extract_date(self, text: str) -> str:
        if not text:
            return "Non spécifiée"

        text = text.strip()

        match = re.search(r'(\d{4})\s*[-–]\s*(\d{4})', text)
        if match:
            return f"{match.group(1)}-{match.group(2)}"

        match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
        if match:
            return match.group(1)

        match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
        if match:
            return match.group(1)

        if any(nd in text.lower() for nd in ['ohne datum', 'undatiert', 's.d.']):
            return "Non datée"

        return "Non spécifiée"

    _ARCHIVE_WORDS = {
        'stadtarchiv', 'kreisarchiv', 'landesarchiv', 'hauptstaatsarchiv',
        'bundesarchiv', 'archiv', 'sammlung', 'bibliothek', 'bürgerinitiativen',
        'staatsarchiv', 'universitätsarchiv', 'bezirksarchiv', 'gemeindearchiv',
    }

    _ARCHIVE_STRIP = re.compile(
        r'^(?:[A-ZÄÖÜ][a-zäöüß]+(?:s|es|isches?|ische|er|ern)\s+)?'
        r'[A-Za-zäöüÄÖÜß]*[Aa]rchiv\w*\s*'
        r'(?:des\s+)?(?:Landkreises?\s+|Kreises?\s+)?'
    )

    _ADJEKTIV_LAND = {
        r'[Bb]ayer': 'Bayern',
        r'[Ss]ächs': 'Sachsen',
        r'[Bb]randenburg': 'Brandenburg',
        r'[Hh]ess': 'Hessen',
        r'[Tt]hüring': 'Thüringen',
        r'[Nn]iedersächs': 'Niedersachsen',
        r'[Mm]ecklenb': 'Mecklenburg-Vorpommern',
        r'[Ww]estfäl': 'Nordrhein-Westfalen',
        r'[Ss]aarländ': 'Saarland',
        r'[Ss]chlwig|[Ss]chleswig': 'Schleswig-Holstein',
        r'[Hh]amburg': 'Hamburg',
        r'[Bb]remer': 'Bremen',
        r'[Bb]erliner': 'Berlin',
    }

    _INSTITUTION_LOCATION = {
        'Bundesarchiv': 'Deutschland',
        'FFBIZ': 'Berlin',
        'Archiv für Diakonie': 'Berlin',
        'Akademie der Künste': 'Berlin',
        'Digitales Deutsches Frauenarchiv': 'Berlin',
        'KIT-Archiv': 'Karlsruhe',
    }

    def _loc_from_archive_name(self, name: str) -> Optional[str]:
        stripped = self._ARCHIVE_STRIP.sub('', name).strip()
        stripped = re.sub(r'^(?:des|der|für|im|am|bei)\s+', '', stripped).strip()
        stripped = re.sub(r'\s*[\(\[\.]{1,}.*', '', stripped, flags=re.DOTALL).strip()
        if stripped and len(stripped) > 2 and stripped[0].isupper() and stripped.lower() not in self._ARCHIVE_WORDS:
            return stripped
        for pattern, land in self._ADJEKTIV_LAND.items():
            if re.search(pattern, name):
                return land
        return None

    def extract_location_ner(self, meta_text: str, institution: str, titre: str = "") -> str:
        """Extrait le lieu via NER spaCy, d'abord sur l'institution puis sur le meta."""
        if institution and any(w in institution.lower() for w in ('archiv', 'bibliothek')):
            result = self._loc_from_archive_name(institution)
            if result:
                return result

        for text in (institution, f"{institution} {meta_text}", titre):
            doc = self.nlp(text)

            for ent in doc.ents:
                if ent.label_ not in ('LOC', 'GPE', 'ORG'):
                    continue

                name = ent.text.strip()

                if any(w in name.lower() for w in ('archiv', 'bibliothek')):
                    result = self._loc_from_archive_name(name)
                    if result:
                        return result
                    continue

                if ent.label_ == 'ORG':
                    continue

                if len(name) <= 2 or name.lower() in self._ARCHIVE_WORDS:
                    continue
                if not name[0].isupper():
                    continue
                if re.search(r'\d', name):
                    continue
                if re.match(r'^[A-ZÄÖÜ][\s\-]', name):
                    continue
                if len(name) <= 8 and len(name) >= 2 and name[1].isupper():
                    continue
                if re.search(r'\.{2,}', name):
                    continue

                name = re.sub(r'^(?:Landkreises?|Kreises?)\s+', '', name).strip()
                if not name or len(name) <= 2:
                    continue

                if ent.end < len(doc) and doc[ent.end].text in ('Kreis', 'Land'):
                    name = f"{name} {doc[ent.end].text}"

                return name

        for key, loc in self._INSTITUTION_LOCATION.items():
            if key in institution or key in meta_text or key in titre:
                return loc

        return "Non spécifié"

    def extract_institution(self, text: str) -> str:
        match = re.search(r'^\s*[\d\-–\s\.]+,\s*([^,]+(?:,[^,]+)?)', text)
        if match:
            institution = match.group(1).strip()
            if re.search(r'(?:archiv|bibliothek|museum|institut|sammlung)', institution, re.IGNORECASE):
                return institution

        patterns = [
            r'((?:Stadt|Landes|Bundes|Kreis|Universitäts)[a-zäöüß]*archiv[^,\n]*)',
            r'(Archiv\s+(?:der|des|für|im)[^,\n]+)',
            r'([A-ZÄÖÜ][a-zäöüß]+(?:stadt|Stadt)\s+[A-ZÄÖÜ][a-zäöüß]+\s+[^,\n]*[Aa]rchiv[^,\n]*)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return ""

    def parse_list_item(self, item_html: str, base_url: str) -> Optional[Initiative]:
        soup = BeautifulSoup(item_html, 'html.parser')

        link = soup.find('a', href=re.compile(r'/item/'))
        if not link:
            return None

        titre = re.sub(r'\s+', ' ', link.get_text(separator=' ')).strip()
        url = urljoin(base_url, link.get('href', ''))

        subtitle_div = soup.find('div', class_='subtitle')
        meta_text = subtitle_div.get_text(' ', strip=True) if subtitle_div else ""

        periode = self.extract_date(meta_text)
        institution = self.extract_institution(meta_text)
        lieu = self.extract_location_ner(meta_text, institution, titre)

        return Initiative(
            titre=titre,
            periode=periode,
            lieu=lieu,
            url=url,
            institution=institution
        )

    async def parse_list_page(self, html: str, page_url: str = "") -> list[tuple[Initiative, str]]:
        soup = BeautifulSoup(html, 'html.parser')
        results = []

        for link in soup.find_all('a', href=re.compile(r'/item/')):
            if not link.get_text(strip=True):
                continue
            parent = link.find_parent(['li', 'div', 'article', 'tr'])
            if parent:
                item_html = str(parent)
            else:
                item_html = str(link.parent) if link.parent else str(link)

            initiative = self.parse_list_item(item_html, BASE_URL)
            if initiative:
                results.append((initiative, item_html))
            else:
                item_url = link.get('href', '')
                self.parse_failures.append({
                    'url': urljoin(BASE_URL, item_url),
                    'page_source': page_url,
                    'raison': 'parsing_failed'
                })

        return results

    async def fetch_oai_location(self, item_id: str) -> Optional[str]:
        oai_url = (
            "https://oai.deutsche-digitale-bibliothek.de/"
            f"?verb=GetRecord&metadataPrefix=ddb&identifier={item_id}"
        )
        xml_text = await self.fetch(oai_url)
        if not xml_text:
            return None
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return None

        EDM = 'http://www.europeana.eu/schemas/edm/'
        for dp in root.iter(f'{{{EDM}}}dataProvider'):
            name = (dp.text or '').strip()
            if not name:
                continue
            lieu = self.extract_location_ner(name, name)
            if lieu != "Non spécifié":
                return lieu

        return None

    async def fetch_detail_location(self, url: str) -> Optional[str]:
        html = await self.fetch(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        for a in soup.find_all('a', href=lambda h: h and '/organization/' in h):
            text = a.get_text(strip=True)
            if not text:
                continue
            lieu = self.extract_location_ner(text, text)
            if lieu != "Non spécifié":
                return lieu

        match = re.search(r'/item/([A-Z0-9]+)', url)
        if match:
            return await self.fetch_oai_location(match.group(1))

        return None

    async def enrich_missing_locations(self) -> int:
        no_loc = [i for i in self.results if i.lieu == "Non spécifié"]
        if not no_loc:
            return 0

        print(f"\n[3/3] Fallback pages de détail ({len(no_loc)} items sans lieu)...")
        found = 0
        pbar = tqdm(total=len(no_loc), desc="      Fallback", unit="item")

        async def process_detail(initiative: Initiative):
            nonlocal found
            lieu = await self.fetch_detail_location(initiative.url)
            if lieu:
                initiative.lieu = lieu
                found += 1
            pbar.update(1)

        batch_size = 10
        for i in range(0, len(no_loc), batch_size):
            batch = no_loc[i:i + batch_size]
            await asyncio.gather(*[process_detail(init) for init in batch])

        pbar.close()
        return found

    def add_result(self, initiative: Initiative) -> bool:
        key = initiative.hash_key()
        if key in self.seen_hashes:
            self.duplicates.append({
                'titre': initiative.titre,
                'url': initiative.url,
            })
            return False
        self.seen_hashes.add(key)
        self.results.append(initiative)
        return True

    async def scrape_all(self) -> list[Initiative]:
        print("\n[1/3] Récupération du nombre total de résultats...")
        total = await self.get_total_results()
        if total == 0:
            print("Error: dans scrape_all")
            return []

        print(f"      -> {total} résultats à traiter")
        print(f"\n[2/3] Extraction des données depuis les pages de liste...")

        pages = (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
        urls = [
            f"{SEARCH_URL}?lang=en&query={quote(QUERY)}&offset={i * ROWS_PER_PAGE}&rows={ROWS_PER_PAGE}"
            for i in range(pages)
        ]

        pbar = tqdm(total=total, desc="      Extraction", unit="item")

        async def process_list_page(url: str):
            html = await self.fetch(url)
            if html:
                items = await self.parse_list_page(html, page_url=url)
                for item, item_html in items:
                    if self.add_result(item):
                        pbar.update(1)

        batch_size = 10
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            await asyncio.gather(*[process_list_page(url) for url in batch])

        pbar.close()

        found_via_detail = await self.enrich_missing_locations()

        remaining = sum(1 for i in self.results if i.lieu == "Non spécifié")
        print(f"\n{'=' * 60}")
        print(f"  Done: {len(self.results)} initiatives extraites")
        print(f"  Lieux trouvés via fallback: {found_via_detail}")
        print(f"  Lieux toujours manquants:   {remaining}")
        missing = len(self.errors) + len(self.duplicates) + len(self.parse_failures)
        if missing > 0:
            print(f"  Manquants: {missing}")
            print(f"    - Doublons: {len(self.duplicates)}")
            print(f"    - Erreurs réseau: {len(self.errors)}")
            print(f"    - Échecs parsing: {len(self.parse_failures)}")
        print(f"{'=' * 60}")

        return self.results

    def export_csv(self, filepath: Path):
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['titre', 'periode', 'lieu', 'institution', 'url'])
            writer.writeheader()
            for init in self.results:
                writer.writerow(init.to_dict())
        print(f"\n  CSV exporté: {filepath}")

async def main():
    parser = argparse.ArgumentParser(
        description="Scraper Archivportal-D pour Bürgerinitiativen",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python scraper.py                       Mode rapide
  python scraper.py --output mon_fichier  Nom de fichier personnalisé
        """
    )
    parser.add_argument('--output', '-o', default='burgerinitiativen',
                        help="Nom du fichier de sortie (sans extension)")

    args = parser.parse_args()

    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)

    async with ArchivportalScraper() as scraper:
        await scraper.scrape_all()

        if scraper.results:
            base_path = output_dir / args.output
            scraper.export_csv(base_path.with_suffix('.csv'))

if __name__ == "__main__":
    asyncio.run(main())
