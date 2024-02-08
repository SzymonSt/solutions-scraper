import os
import pandas as pd
import time
from pyarrow import parquet

class Extractor:
    def __init__(self, path, columns_to_extract_from=None):
        self.path = path
        # For now only one column is supported
        self.columns = columns_to_extract_from

        # Extractor hyperparameters
        self.stopchars = ["`","```"]
        self.min_char_num = 120
        self.keywords = ["error", "exception", "fail", "warning", "critical", "fatal", "stacktrace", 
                         "traceback", "issue", "crash", "hang", "freeze",
                         "timeout", "deadlock", "corrupt", "invalid", "illegal", "unhandled", "uncaught", 
                         "unexpected", "unimplemented", "unsupported", "missing", "invalid", "illegal", 
                         "unauthorized", "denied", "forbidden", "blocked", "rejected", "panic", "abort"]

    def extract(self):
        print(f"Extracting log files from {self.path}...")
        files = os.listdir(self.path)
        for file_name in files:
            print(f"Extracting from {file_name}...")
            try:
                self._extract_from_file(file_name)
            except Exception as e:
                print(f"Error extracting from {file_name}: {e}")
    
    def _extract_from_file(self, file_name):
        extracted_logs = []
        file = parquet.ParquetFile(os.path.join(self.path, file_name))
        batch = file.iter_batches(
            batch_size=25000,
            columns=self.columns,
        )
        try:
            while (record := next(batch).to_pydict()):
                    for r in record[self.columns[0]]:
                        logs = self._process_content(r)
                        extracted_logs.extend(logs)
        except StopIteration:
            pass
        
        success = False
        while not success:
            success = self._save_batch(extracted_logs, file_name)
            if success:
                print(f"Extracted {len(extracted_logs)} logs from {file_name}")
                extracted_logs.clear()

    def _process_content(self, content) -> list:
        pass

    def _save_batch(self, logs, file_name) -> bool:
        try:
            df = pd.DataFrame(logs, columns=["logs"])
            df.to_parquet(f"./extracted_logs/{file_name}.parquet")
        except Exception as e:
            print(f"Error saving batch to file {file_name}: {e}")
            time.sleep(5)
            return False
        
        return True



def main():
    file_path = "D:\\Projects\\the-stack-github-issues\\data"
    columns = ["content"]
    ext = Extractor(file_path, columns)
    ext.extract()


if __name__ == "__main__":
    main()