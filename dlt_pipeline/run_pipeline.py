import json
import re
from io import BytesIO, StringIO
import os
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import dlt


XPT_BASE_URL = "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/{cycle_start_year}/DataFiles/{file_code}.xpt"
MORTALITY_INDEX_URL = "https://www.cdc.gov/nchs/linked-data/mortality-files/index.html"
MORTALITY_LISTING_URL = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/datalinkage/linked_mortality/"

CYCLES = [
    {"cycle_start_year": 2003, "cycle_end_year": 2004, "suffix": "C"},
    {"cycle_start_year": 2005, "cycle_end_year": 2006, "suffix": "D"},
    {"cycle_start_year": 2007, "cycle_end_year": 2008, "suffix": "E"},
    {"cycle_start_year": 2009, "cycle_end_year": 2010, "suffix": "F"},
    {"cycle_start_year": 2011, "cycle_end_year": 2012, "suffix": "G"},
    {"cycle_start_year": 2013, "cycle_end_year": 2014, "suffix": "H"},
    {"cycle_start_year": 2015, "cycle_end_year": 2016, "suffix": "I"},
    {"cycle_start_year": 2017, "cycle_end_year": 2018, "suffix": "J"},
]

ASSET_DEFINITIONS = {
    "demo": {"template": "DEMO_{suffix}", "file_format": "XPT"},
    "dr1tot": {"template": "DR1TOT_{suffix}", "file_format": "XPT"},
    "dr2tot": {"template": "DR2TOT_{suffix}", "file_format": "XPT"},
    "dr1iff": {"template": "DR1IFF_{suffix}", "file_format": "XPT"},
    "dr2iff": {"template": "DR2IFF_{suffix}", "file_format": "XPT"},
    "glu": {
        "template": "GLU_{suffix}",
        "file_format": "XPT",
        "overrides": {"C": "L10AM_C"},
    },
}

MORTALITY_COLSPECS = [
    (0, 14),   # SEQN
    (14, 15),  # ELIGSTAT
    (15, 16),  # MORTSTAT
    (16, 19),  # UCOD_LEADING
    (19, 20),  # DIABETES
    (20, 21),  # HYPERTEN
    (42, 45),  # PERMTH_INT
    (45, 48),  # PERMTH_EXM
]
MORTALITY_COLUMNS = [
    "SEQN", "ELIGSTAT", "MORTSTAT", "UCOD_LEADING", "DIABETES", "HYPERTEN", "PERMTH_INT", "PERMTH_EXM"
]


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "kestra-nhanes-low-protein-ingestion/1.0",
            "Accept": "*/*",
        }
    )
    return session


def fetch_bytes(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=180)
    response.raise_for_status()
    return response.content


def fetch_text(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=180)
    response.raise_for_status()
    return response.text


def read_xpt(session: requests.Session, url: str) -> pd.DataFrame:
    content = fetch_bytes(session, url)
    df = pd.read_sas(BytesIO(content), format="xport")
    return df


def read_mortality_file(session: requests.Session, url: str) -> pd.DataFrame:
    text = fetch_bytes(session, url).decode("utf-8", errors="ignore")
    df = pd.read_fwf(
        StringIO(text),
        colspecs=MORTALITY_COLSPECS,
        names=MORTALITY_COLUMNS,
        dtype=str,
    )
    for column in ["SEQN", "ELIGSTAT", "MORTSTAT", "DIABETES", "HYPERTEN", "PERMTH_INT", "PERMTH_EXM"]:
        df[column] = pd.to_numeric(df[column].str.strip(), errors="coerce")
    df["UCOD_LEADING"] = df["UCOD_LEADING"].astype("string").str.strip()
    return df


def add_common_metadata(df: pd.DataFrame, cycle: dict, source_url: str, file_code: str, asset_type: str) -> pd.DataFrame:
    standardized = df.copy()
    standardized.columns = [str(col).upper() for col in standardized.columns]
    standardized["CYCLE_START_YEAR"] = cycle["cycle_start_year"]
    standardized["CYCLE_END_YEAR"] = cycle["cycle_end_year"]
    standardized["CYCLE_LABEL"] = f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}"
    standardized["FILE_CODE"] = file_code
    standardized["ASSET_TYPE"] = asset_type
    standardized["SOURCE_URL"] = source_url
    return standardized


def mortality_release_year_for_cycle(listing_html: str, cycle_start_year: int, cycle_end_year: int) -> int:
    pattern = f"NHANES_{cycle_start_year}_{cycle_end_year}_MORT_" + r"(\d{4})_PUBLIC\.dat"
    release_years = re.findall(pattern, listing_html)
    if not release_years:
        raise RuntimeError(
            f"Could not determine the latest public-use mortality release year for NHANES {cycle_start_year}-{cycle_end_year}."
        )
    return max(int(year) for year in release_years)


def mortality_followup_text(index_html: str) -> str | None:
    match = re.search(
        r"updated with mortality follow-up data through ([^.]+?\d{4})",
        index_html,
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else None


@dlt.source(name="nhanes_source")
def nhanes_source():
    session = make_session()
    mortality_index_html = fetch_text(session, MORTALITY_INDEX_URL)
    mortality_listing_html = fetch_text(session, MORTALITY_LISTING_URL)
    mortality_followup_available_through = mortality_followup_text(mortality_index_html)
    
    cycle_release_years = {}
    for cycle in CYCLES:
        cycle_public_release_year = mortality_release_year_for_cycle(
            mortality_listing_html,
            cycle["cycle_start_year"],
            cycle["cycle_end_year"],
        )
        cycle_release_years[f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}"] = cycle_public_release_year

    # Pre-populate source catalog records
    source_records = []
    for asset_type, asset_definition in ASSET_DEFINITIONS.items():
        for cycle in CYCLES:
            file_code = asset_definition.get("overrides", {}).get(
                cycle["suffix"],
                asset_definition["template"].format(suffix=cycle["suffix"]),
            )
            source_url = XPT_BASE_URL.format(
                cycle_start_year=cycle["cycle_start_year"],
                file_code=file_code,
            )
            source_records.append({
                "asset_type": asset_type,
                "file_code": file_code,
                "file_format": asset_definition["file_format"],
                "cycle_start_year": cycle["cycle_start_year"],
                "cycle_end_year": cycle["cycle_end_year"],
                "cycle_label": f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}",
                "source_url": source_url,
                "mortality_public_release_year": None,
                "mortality_followup_available_through": mortality_followup_available_through,
                "paper_comparability_note": "2014 Levine paper used NHANES III, so this continuous NHANES 2003-2018 pipeline is a validation extension rather than an exact reproduction.",
            })

    for cycle in CYCLES:
        cycle_public_release_year = cycle_release_years[f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}"]
        mortality_file_name = f"NHANES_{cycle['cycle_start_year']}_{cycle['cycle_end_year']}_MORT_{cycle_public_release_year}_PUBLIC.dat"
        mortality_url = MORTALITY_LISTING_URL + mortality_file_name
        source_records.append({
            "asset_type": "mortality",
            "file_code": mortality_file_name.replace(".dat", ""),
            "file_format": "DAT",
            "cycle_start_year": cycle["cycle_start_year"],
            "cycle_end_year": cycle["cycle_end_year"],
            "cycle_label": f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}",
            "source_url": mortality_url,
            "mortality_public_release_year": cycle_public_release_year,
            "mortality_followup_available_through": mortality_followup_available_through,
            "paper_comparability_note": "2014 Levine paper used NHANES III, so this continuous NHANES 2003-2018 pipeline is a validation extension rather than an exact reproduction.",
        })

    # Define generators for each table
    for asset_type, asset_definition in ASSET_DEFINITIONS.items():
        @dlt.resource(name=f"nhanes_lp_{asset_type}_2003_2018", write_disposition="replace")
        def get_xpt_asset(asset_type=asset_type, asset_definition=asset_definition):
            for cycle in CYCLES:
                file_code = asset_definition.get("overrides", {}).get(
                    cycle["suffix"],
                    asset_definition["template"].format(suffix=cycle["suffix"]),
                )
                source_url = XPT_BASE_URL.format(
                    cycle_start_year=cycle["cycle_start_year"],
                    file_code=file_code,
                )
                print(f"Yielding {asset_type} for {cycle['cycle_start_year']}-{cycle['cycle_end_year']} from {source_url}...")
                frame = read_xpt(session, source_url)
                frame = add_common_metadata(frame, cycle, source_url, file_code, asset_type)
                yield frame
        yield get_xpt_asset

    @dlt.resource(name="nhanes_lp_mortality_2003_2018", write_disposition="replace")
    def get_mortality():
        for cycle in CYCLES:
            cycle_public_release_year = cycle_release_years[f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}"]
            mortality_file_name = (
                f"NHANES_{cycle['cycle_start_year']}_{cycle['cycle_end_year']}"
                f"_MORT_{cycle_public_release_year}_PUBLIC.dat"
            )
            mortality_url = MORTALITY_LISTING_URL + mortality_file_name
            print(f"Yielding mortality for {cycle['cycle_start_year']}-{cycle['cycle_end_year']} from {mortality_url}...")
            mortality_frame = read_mortality_file(session, mortality_url)
            mortality_frame = add_common_metadata(
                mortality_frame,
                cycle,
                mortality_url,
                mortality_file_name.replace(".dat", ""),
                "mortality",
            )
            mortality_frame["MORTALITY_PUBLIC_RELEASE_YEAR"] = cycle_public_release_year
            yield mortality_frame
    yield get_mortality

    @dlt.resource(name="nhanes_lp_source_catalog_2003_2018", write_disposition="replace")
    def get_source_catalog():
        print("Yielding source catalog...")
        yield pd.DataFrame(source_records).sort_values(by=["cycle_start_year", "asset_type", "file_code"])
    yield get_source_catalog

    # Generate Manifest (as a side effect, saving to disk for Kestra to upload)
    manifest = {
        "dataset_window": "2003-2018",
        "cycles": [f"{cycle['cycle_start_year']}-{cycle['cycle_end_year']}" for cycle in CYCLES],
        "nhanes_assets": list(ASSET_DEFINITIONS.keys()),
        "public_mortality_release_year_by_cycle": cycle_release_years,
        "mortality_followup_available_through": mortality_followup_available_through,
        "linked_mortality_note": "CDC currently exposes NHANES linked mortality for survey years 1999-2018; the public-use files are distributed as the latest available public release on the linked mortality FTP listing.",
        "paper_comparability_note": "The original 2014 Levine paper analyzed NHANES III rather than continuous NHANES 2003-2018.",
    }
    Path("metadata").mkdir(parents=True, exist_ok=True)
    Path("metadata/manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="nhanes_lp",
        destination="bigquery",
        dataset_name=os.environ.get("GCP_DATASET", "nhanes"),
    )
    
    print("Running DLT pipeline for NHANES CDC data extraction...")
    load_info = pipeline.run(nhanes_source())
    print("DLT pipeline complete!")
    print(load_info)


if __name__ == "__main__":
    main()
