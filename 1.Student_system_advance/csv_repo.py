import csv
from pathlib import Path
from typing import List, Dict, Optional

class Student_Repository:
    # "simple csv file student repository"
    def __init__(self, file_path: Path | None = None):
        # default: stu.csv in current directory
        self.file_path = file_path or Path(__file__).with_name("stu.csv")
        self.headers = ["id", "name", "math", "english"]
        self._ensure_csv()

    def _ensure_csv(self) -> None:
        # create csv file if not exist
        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with self.file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()

    def _read_all(self) -> List[Dict[str, str]]:
        # read all students from csv file
        with self.file_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [row for row in reader]
    
    def _write_all(self, rows: List[Dict[str, str]]) -> None:
        # write all students to csv file
        with self.file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()
            writer.writerows(rows)

    # add, delete, update, query, etc. functions here...
    def add(self, sid: str, name: str, math:str | float | None, english:str | float | None) -> None:
        # add a new student to csv file
        sid = sid.strip()
        if not sid:
            raise ValueError("student id cannot be empty")
        rows = self._read_all()
        if any(row["id"] == sid for row in rows):
            raise ValueError(f"student id {sid} already exists")
        row = {"id": sid, "name": name.strip(), "math": str(math), "english": str(english)}
        rows.append(row)
        self._write_all(rows)

    def get(self, sid: str) -> Optional[Dict[str, str]]:
        # get a student by id
        sid = sid.strip()
        for r in self._read_all():
            if r["id"] == sid:
                return r
        return None

    def list(self) -> List[Dict[str, str]]:
        # list all students
        return self._read_all().sort(key=lambda r: r["id"])
    
    def update(self, sid: str, name: str | None = None, math: str | float | None = None, english: str | float | None = None) -> bool:
        # update a student by id
        sid = sid.strip()
        rows = self._read_all()
        changed = False
        for r in rows:
            if r["id"] == sid:
                if name is not None:
                    r["name"] = name.strip()
                if math is not None:
                    r["math"] = "" if math is None or math == "" else str(math)
                if english is not None:
                    r["english"] = "" if english is None or english == "" else str(english)
                changed = True
                break
        if changed:
            self._write_all(rows)
        
        return changed
    
    def delete(self, sid: str) -> bool:
        # delete a student by id
        sid = sid.strip()
        rows = self._read_all()
        new_rows = [r for r in rows if r["id"] != sid]
        if len(rows) != len(new_rows):
            self._write_all(new_rows)
            return True
        else:
            return False
    
    def upsert(self, sid: str, name: str | None = None, math: str | float | None = None, english: str | float | None = None) -> None:
        # add or update a student by id
        if self.get(sid) is None:
            self.add(sid, name, math, english)
        else:
            self.update(sid, name, math, english)
