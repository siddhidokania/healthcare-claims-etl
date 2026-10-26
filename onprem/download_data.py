"""Download CMS DE-SynPUF Sample 1 (synthetic Medicare claims, public domain).

Usage:
    python onprem/download_data.py [--data-dir data/raw]

Files (about 51 MB zipped, 222 MB unzipped):
    Beneficiary summary 2008, 2009, 2010
    Inpatient claims 2008-2010
    Outpatient claims 2008-2010

Source page:
https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1
"""

import argparse
import urllib.request
import zipfile
from pathlib import Path

BASE = "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads"
FILES = [
    f"{BASE}/de1_0_2008_beneficiary_summary_file_sample_1.zip",
    f"{BASE}/de1_0_2009_beneficiary_summary_file_sample_1.zip",
    # CMS publishes the 2010 Sample 1 file under this name; it contains
    # DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv.
    "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/synpufs/downloads/de1_0_2010_beneficiary_summary_file_sample_20.zip",
    f"{BASE}/de1_0_2008_to_2010_inpatient_claims_sample_1.zip",
    f"{BASE}/de1_0_2008_to_2010_outpatient_claims_sample_1.zip",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/raw")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    zip_dir = data_dir / "zips"
    zip_dir.mkdir(parents=True, exist_ok=True)

    for url in FILES:
        target = zip_dir / url.rsplit("/", 1)[-1]
        if not target.exists():
            print(f"Downloading {target.name}")
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request) as response, open(target, "wb") as out:
                out.write(response.read())
        with zipfile.ZipFile(target) as archive:
            archive.extractall(data_dir)
            print(f"Extracted {', '.join(archive.namelist())}")


if __name__ == "__main__":
    main()
