import re
import fitz
import json
import logging
import pandas as pd
import json, hashlib
from pathlib import Path
logger = logging.getLogger(__name__)


def extract_records_from_file(file_path: Path):
    try:
        file_path = Path(file_path)
        ext = file_path.suffix.lower()
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        elif ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext == ".json":
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        elif ext == ".txt":
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
            records = []
            for line in lines:
                pairs = re.findall(r"(\w+)\s*:\s*([\w\s]+)", line)
                if pairs:
                    records.append({k: v.strip() for k, v in pairs})
            return records
        elif ext == ".pdf":
            with fitz.open(file_path) as doc:
                text = "\n".join(page.get_text() for page in doc)
            pairs = re.findall(r"(\w+)\s*:\s*([\w\s]+)", text)
            if pairs:
                return [{k: v.strip() for k, v in pairs}]
            amounts = re.findall(r"\d+\.\d{2}", text)
            dates = re.findall(r"\d{4}-\d{2}-\d{2}", text)
            return [{
                "merchant": "Unknown",
                "amount": float(amounts[-1]) if amounts else 0,
                "date": dates[0] if dates else "Unknown"
            }]
        elif ext == ".parquet":
            df = pd.read_parquet(file_path)
        elif ext in [".html", ".htm"]:
            df = pd.read_html(file_path)[0]
        elif ext == ".xml":
            df = pd.read_xml(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_path}")
            return []
        df.columns = [str(c).strip() for c in df.columns]
        records = df.to_dict("records")
        return records
    except Exception as e:
        logger.error(f"Error extracting records from '{file_path}': {e}")
        return []