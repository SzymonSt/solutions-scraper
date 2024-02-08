import os
import pandas as pd
import time
from pyarrow import parquet

class Extractor:
    def __init__(self, path, columns_to_extract_from=None, output_file_prefix="extracted_logs"):
        self.path = path
        self.output_file_prefix = output_file_prefix
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
            self._extract_from_file(file_name)
    
    def _extract_from_file(self, file_name):
        extracted_logs = []
        file = parquet.ParquetFile(os.path.join(self.path, file_name))
        batch = file.iter_batches(
            batch_size=25000,
            columns=self.columns,
        )
        try:
            while (record := next(batch).to_pydict()):
                    for i, r in enumerate(record[self.columns[0]]):
                        if i % 1000 == 0:
                            print(f"{i/len(record[self.columns[0]])*100:.2f}% progress...")
                        logs = self._process_content(r)
                        if logs:
                            extracted_logs.extend(logs)
        except StopIteration:
            pass
        
        success = False
        while not success:
            success = self._save_batch(extracted_logs, file_name)
            if success:
                print(f"Extracted {len(extracted_logs)} logs from {file_name}")
                extracted_logs.clear()

    def _process_content(self, content: str) -> list:
        candidates = []
        for sc in self.stopchars:
            if sc in content:
                candidates.extend(self._find_log_markup(content, sc))

        logs = [c for c in candidates if len(c) > self.min_char_num and any(k in c.lower() for k in self.keywords)]
        return logs

    def _find_log_markup(self, content: str, stop_word: str) -> list:
        candidates = []
        lidx = 0
        while lidx < len(content):
            try:
                idx = content.index(stop_word, lidx)
            except ValueError:
                break
            if idx:
                try:
                    tmplidx = content.index(stop_word, idx+1)
                except ValueError:
                    break
                if tmplidx:
                    lidx = tmplidx + 1
                    candidates.append(content[idx:tmplidx]) 
            else:
                break
        return candidates

    def _save_batch(self, logs, file_name) -> bool:
        try:
            df = pd.DataFrame(logs, columns=["logs"])
            df.to_parquet(f"./extracted_logs/{self.output_file_prefix}{file_name}.parquet")
        except Exception as e:
            print(f"Error saving batch to file {file_name}: {e}")
            time.sleep(5)
            return False
        
        return True



def main():
    file_path = "D:\\Projects\\the-stack-github-issues\\data"
    columns = ["content"]
    output_file_prefix = "gh__"
    ext = Extractor(file_path, columns, output_file_prefix)
    ext.extract()


if __name__ == "__main__":
    main()