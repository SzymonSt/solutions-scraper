import os
import pandas as pd
from pyarrow import parquet

class Extractor:
    def __init__(self, path, columns_to_extract_from=None):
        self.path = path
        self.columns = columns_to_extract_from

    def extract(self):
        print(f"Extracting log files from {self.path}...")
        files = os.listdir(self.path)
        for file_name in files:
            print(f"Extracting from {file_name}...")
            self._extract_from_file(file_name)
    
    def _extract_from_file(self, file_name):
        file = parquet.ParquetFile(os.path.join(self.path, file_name))
        batch = file.iter_batches(
            batch_size=5000,
            columns=self.columns,
        )
        
        record = next(batch).to_pydict()
        content = record[self.columns[0]]



def main():
    file_path = "D:\\Projects\\the-stack-github-issues\\data"
    columns = ["content"]
    ext = Extractor(file_path, columns)


if __name__ == "__main__":
    main()