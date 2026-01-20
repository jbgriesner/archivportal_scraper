#!/usr/bin/env python3

import asyncio
import aiohttp
import argparse
import csv
import hashlib
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, quote

try:
    from bs4 import BeautifulSoup
    from tqdm import tqdm
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


GERMAN_CITIES = {
    'berlin', 'hamburg', 'münchen', 'munich', 'köln', 'cologne', 'frankfurt',
    'stuttgart', 'düsseldorf', 'dortmund', 'essen', 'leipzig', 'bremen',
    'dresden', 'hannover', 'nürnberg', 'duisburg', 'bochum', 'wuppertal',
    'bielefeld', 'bonn', 'münster', 'karlsruhe', 'mannheim', 'augsburg',
    'wiesbaden', 'gelsenkirchen', 'mönchengladbach', 'braunschweig', 'chemnitz',
    'kiel', 'aachen', 'halle', 'magdeburg', 'freiburg', 'krefeld', 'lübeck',
    'oberhausen', 'erfurt', 'mainz', 'rostock', 'kassel', 'hagen', 'hamm',
    'saarbrücken', 'mülheim', 'potsdam', 'ludwigshafen', 'oldenburg', 'leverkusen',
    'osnabrück', 'solingen', 'heidelberg', 'herne', 'neuss', 'darmstadt',
    'paderborn', 'regensburg', 'ingolstadt', 'würzburg', 'wolfsburg', 'ulm',
    'heilbronn', 'pforzheim', 'göttingen', 'bottrop', 'trier', 'recklinghausen',
    'reutlingen', 'bremerhaven', 'koblenz', 'bergisch gladbach', 'jena',
    'remscheid', 'erlangen', 'moers', 'siegen', 'hildesheim', 'salzgitter',
    'dormagen', 'wertheim', 'aichelberg', 'wyhl', 'gorleben', 'brokdorf',
    'kalkar', 'wackersdorf', 'grohnde', 'biblis', 'neckarwestheim',
    # Régions
    'baden-württemberg', 'bayern', 'bavaria', 'brandenburg', 'hessen',
    'mecklenburg-vorpommern', 'niedersachsen', 'nordrhein-westfalen', 'nrw',
    'rheinland-pfalz', 'saarland', 'sachsen', 'sachsen-anhalt', 'schleswig-holstein',
    'thüringen', 'rhein-kreis', 'schwarzwald', 'eifel', 'hunsrück', 'taunus',
}


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
        """Clé unique pour déduplication."""
        key = f"{self.titre.lower().strip()}|{self.periode}|{self.lieu.lower().strip()}"
        return hashlib.md5(key.encode()).hexdigest()


class ArchivportalScraper:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.results: list[Initiative] = []
        self.seen_hashes: set[str] = set()
        self.errors: list[dict] = []  # Erreurs réseau
        self.duplicates: list[dict] = []  # Doublons ignorés
        self.parse_failures: list[dict] = []  # Échecs de parsing
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

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

    def extract_location(self, text: str, title: str = "", context: str = "") -> str:
        combined = f"{title} {text} {context}".lower()

        found_cities = []
        for city in GERMAN_CITIES:
            if city in combined:
                pattern = r'\b' + re.escape(city) + r'\b'
                if re.search(pattern, combined):
                    found_cities.append(city.title())

        if found_cities:
            return found_cities[0]

        location_patterns = [
            r'\bin\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?)',
            r'\bbei\s+([A-ZÄÖÜ][a-zäöüß]+)',
            r'\baus\s+([A-ZÄÖÜ][a-zäöüß]+)',
            r'\bRegion\s+([A-ZÄÖÜ][a-zäöüß]+)',
            r'\bKreis\s+([A-ZÄÖÜ][a-zäöüß]+)',
            r'\bStadt\s+([A-ZÄÖÜ][a-zäöüß]+)',
        ]

        full_text = f"{title} {text} {context}"
        for pattern in location_patterns:
            match = re.search(pattern, full_text)
            if match:
                loc = match.group(1)
                # Filtrer les faux positifs communs
                if loc.lower() not in ['der', 'die', 'das', 'und', 'oder', 'für', 'gegen']:
                    return loc

        archive_match = re.search(r'(?:Stadtarchiv|Landesarchiv|Archiv)\s+([A-ZÄÖÜ][a-zäöüß-]+)', text)
        if archive_match:
            return f"(Archive: {archive_match.group(1)})"

        return "Non spécifié"

    def extract_institution(self, text: str) -> str:
        patterns = [
            r'((?:Stadt|Landes|Bundes)?[Aa]rchiv[^,\n]+)',
            r'(Archiv\s+(?:für|im|der)[^,\n]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()

        return ""

    def parse_list_item(self, item_html: str, base_url: str) -> Optional[Initiative]:
        soup = BeautifulSoup(item_html, 'html.parser')

        link = soup.find('a', href=re.compile(r'/item/'))
        if not link:
            return None

        titre = link.get_text(strip=True)
        url = urljoin(base_url, link.get('href', ''))

        full_text = soup.get_text(' ', strip=True)

        meta_text = full_text.replace(titre, '', 1).strip()

        periode = self.extract_date(meta_text)
        lieu = self.extract_location(meta_text, titre)
        institution = self.extract_institution(meta_text)

        return Initiative(
            titre=titre,
            periode=periode,
            lieu=lieu,
            url=url,
            institution=institution
        )

    async def parse_list_page(self, html: str, page_url: str = "") -> list[Initiative]:
        soup = BeautifulSoup(html, 'html.parser')
        results = []

        for link in soup.find_all('a', href=re.compile(r'/item/')):
            parent = link.find_parent(['li', 'div', 'article', 'tr'])
            if parent:
                item_html = str(parent)
            else:
                item_html = str(link.parent) if link.parent else str(link)

            initiative = self.parse_list_item(item_html, BASE_URL)
            if initiative:
                results.append(initiative)
            else:
                item_url = link.get('href', '')
                self.parse_failures.append({
                    'url': urljoin(BASE_URL, item_url),
                    'page_source': page_url,
                    'raison': 'parsing_failed'
                })

        return results

    def add_result(self, initiative: Initiative) -> bool:
        key = initiative.hash_key()
        if key in self.seen_hashes:
            self.duplicates.append({
                'titre': initiative.titre,
                'periode': initiative.periode,
                'lieu': initiative.lieu,
                'url': initiative.url,
                'raison': 'doublon'
            })
            return False
        self.seen_hashes.add(key)
        self.results.append(initiative)
        return True

    async def scrape_all(self) -> list[Initiative]:
        print("\n[1/2] Récupération du nombre total de résultats...")
        total = await self.get_total_results()
        if total == 0:
            print("Error: dans scrape_all")
            return []

        print(f"      -> {total} résultats à traiter")

        print(f"\n[2/2] Extraction des données...")

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
                for item in items:
                    if self.add_result(item):
                        pbar.update(1)

        batch_size = 10
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            await asyncio.gather(*[process_list_page(url) for url in batch])

        pbar.close()

        print(f"\n{'=' * 60}")
        print(f"  Done: {len(self.results)} initiatives extraites")
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

    def export_missing(self, filepath: Path):
        all_missing = []

        for dup in self.duplicates:
            all_missing.append({
                'type': 'doublon',
                'url': dup.get('url', ''),
                'titre': dup.get('titre', ''),
                'details': f"periode={dup.get('periode', '')}, lieu={dup.get('lieu', '')}"
            })

        for err in self.errors:
            all_missing.append({
                'type': 'erreur_reseau',
                'url': err.get('url', ''),
                'titre': '',
                'details': err.get('error', f"status={err.get('status', 'unknown')}")
            })

        for fail in self.parse_failures:
            all_missing.append({
                'type': 'parsing_failed',
                'url': fail.get('url', ''),
                'titre': '',
                'details': f"source={fail.get('page_source', '')}"
            })

        if all_missing:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['type', 'url', 'titre', 'details'])
                writer.writeheader()
                writer.writerows(all_missing)
            print(f"  Manquants exportés: {filepath} ({len(all_missing)} entrées)")


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
            scraper.export_missing(output_dir / "missing.csv")

    print(f"\n  Output files in: {output_dir.absolute()}")


if __name__ == "__main__":
    asyncio.run(main())
