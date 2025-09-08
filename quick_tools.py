import os
from pathlib import Path
import pandas as pd
from nodejobs.dependencies.BaseService import BaseService

class ManualTools(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "unpack_excel": {"required_args": ["excel_path","to_csv_path"], "method": cls.unpack_excel},
            "combine_csvs": {"required_args": ["src_dir","out_csv_path"], "method": cls.combine_csvs},  
        }

    @classmethod
    def unpack_excel(cls, excel_path: str, to_csv_path: str):
        src = Path(excel_path)
        dst = Path(to_csv_path)

        if not src.exists():
            raise FileNotFoundError(str(src))

        dst.mkdir(parents=True, exist_ok=True)

        sheets = pd.read_excel(src, sheet_name=None)
        written = []

        for sheet_name, df in sheets.items():
            safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in sheet_name) or "Sheet"
            out_path = dst / f"{safe}.csv"
            df.to_csv(out_path, index=False)
            written.append(str(out_path))

        return {"written": written}

    @classmethod
    def combine_csvs(cls, src_dir: str, out_csv_path: str):  # NEW
        src = Path(src_dir)
        out = Path(out_csv_path)

        if not src.exists() or not src.is_dir():
            raise FileNotFoundError(str(src))

        out.parent.mkdir(parents=True, exist_ok=True)

        csv_files = sorted([p for p in src.glob("*.csv") if p.is_file()], key=lambda p: p.name)
        if len(csv_files) == 0:
            raise ValueError(f"No CSV files found in {src}")

        frames = []
        used = []
        for p in csv_files:
            df = pd.read_csv(p)
            df.insert(len(df.columns), "source", p.name)
            frames.append(df)
            used.append(p.name)

        master = pd.concat(frames, ignore_index=True)
        master.to_csv(out, index=False)
        return {"written": str(out), "rows": int(master.shape[0]), "files": used}


if __name__ == "__main__":
    ManualTools.run_cli()
# usage:
# python3 quick_tools.py combine_csvs src_dir=./data/extract_1/ out_csv_path=./data/extract_2/sensors.csv
