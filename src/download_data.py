
"""
Download a small raw-data subset for the extreme-weather-bigdata project.

Sources:
- NOAA GHCN-Daily station inventory and daily records
- NOAA Storm Events database

Raw files are preserved under data/raw/.
No event thresholds or cleaning are applied here.
"""

import argparse
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from html.parser import HTMLParser


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

GHCN_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
STATIONS_URL = f"{GHCN_BASE}/ghcnd-stations.txt"
GHCN_DAILY_URL = f"{GHCN_BASE}/all"

STORM_EVENTS_URL = (
    "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
)

# Small US test subset. More stations can be added later.
DEFAULT_STATIONS = [
    "USW00094728",  # Central Park, New York
    "USW00023174",  # Los Angeles International Airport
]

USER_AGENT = "extreme-weather-bigdata-research/1.0"


class LinkParser(HTMLParser):
    """Collect links from NOAA's directory listing."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for key, value in attrs:
                if key.lower() == "href" and value:
                    self.links.append(value)


def download_file(url: str, destination: Path) -> bool:
    """Download a URL unless the destination already exists."""
    if destination.exists() and destination.stat().st_size > 0:
        print(f"Already downloaded; skipping: {destination.name}")
        return True

    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT},
    )

    print(f"Downloading: {url}")

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            with destination.open("wb") as output:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)

        print(
            f"Saved: {destination.relative_to(PROJECT_ROOT)} "
            f"({destination.stat().st_size:,} bytes)"
        )
        return True

    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        if destination.exists():
            destination.unlink()
        print(f"Download failed: {url}\nReason: {exc}", file=sys.stderr)
        return False


def download_ghcn(station_ids: list[str]) -> bool:
    """Download station metadata and selected station .dly files."""
    ghcn_dir = RAW_DIR / "ghcn_d"
    ghcn_dir.mkdir(parents=True, exist_ok=True)

    success = download_file(
        STATIONS_URL,
        ghcn_dir / "ghcnd-stations.txt",
    )

    for station_id in station_ids:
        if not re.fullmatch(r"[A-Z0-9]{11}", station_id):
            print(f"Skipping invalid GHCN station ID: {station_id}")
            success = False
            continue

        url = f"{GHCN_DAILY_URL}/{station_id}.dly"
        destination = ghcn_dir / f"{station_id}.dly"

        if not download_file(url, destination):
            success = False

    return success


def download_storm_events(year: int) -> bool:
    """Find and download NOAA Storm Events details for one year."""
    storm_dir = RAW_DIR / "storm_events"
    storm_dir.mkdir(parents=True, exist_ok=True)

    request = urllib.request.Request(
        STORM_EVENTS_URL,
        headers={"User-Agent": USER_AGENT},
    )

    print(f"Looking up NOAA Storm Events files for {year}...")

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            html = response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"Could not read NOAA directory listing: {exc}", file=sys.stderr)
        return False

    parser = LinkParser()
    parser.feed(html)

    pattern = re.compile(
        rf"StormEvents_details-ftp_v1\.0_d{year}_c(\d{{8}})\.csv(?:\.gz)?$"
    )

    matches = []
    for link in parser.links:
        filename = link.rsplit("/", 1)[-1]
        match = pattern.fullmatch(filename)
        if match:
            matches.append((match.group(1), filename))

    if not matches:
        print(
            f"No Storm Events details file found for {year}. "
            "Check NOAA's directory listing or choose another year.",
            file=sys.stderr,
        )
        return False

    # NOAA may publish revised files; select the latest creation-date version.
    _, filename = max(matches, key=lambda item: item[0])
    url = STORM_EVENTS_URL + filename

    return download_file(url, storm_dir / filename)


def main():
    parser = argparse.ArgumentParser(
        description="Download a small NOAA extreme-weather data subset."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="Storm Events data year (default: 2023).",
    )
    parser.add_argument(
        "--stations",
        nargs="+",
        default=DEFAULT_STATIONS,
        help="GHCN-D station IDs to download.",
    )
    args = parser.parse_args()

    if not 1950 <= args.year <= 2100:
        parser.error("--year must be between 1950 and 2100")

    print(f"Project directory: {PROJECT_ROOT}")
    print(f"Raw-data directory: {RAW_DIR}")
    print(f"Storm Events year: {args.year}")
    print(f"Stations: {', '.join(args.stations)}")
    print()

    ghcn_ok = download_ghcn(args.stations)
    storm_ok = download_storm_events(args.year)

    print("\nDownload summary")
    print(f"GHCN-D: {'OK' if ghcn_ok else 'FAILED'}")
    print(f"Storm Events: {'OK' if storm_ok else 'FAILED'}")

    if not (ghcn_ok and storm_ok):
        sys.exit(1)

    print("\nRaw-data download completed.")


if __name__ == "__main__":
    main()