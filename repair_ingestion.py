import os
import subprocess

def get_git_file(commit, filename):
    return subprocess.check_output(['git', 'show', f'{commit}:{filename}']).decode('utf-8')

# Get the old valid file (before truncation)
old_content = get_git_file('ccdd6dab81a35e4486ef6279a93ee8625ea607b6', 'ingestion.py')

# Extract the missing parts (from `if not entries:` inside _dom_at to the end)
missing_parts = ""
capture = False
for line in old_content.split('\n'):
    if 'if not entries:' in line and 'def _dom_at' not in line and not capture:
        # Check if we are inside _dom_at
        capture = True
    
    if capture:
        missing_parts += line + '\n'

# Get the current truncated file
with open('ingestion.py', 'r') as f:
    current_content = f.read()

# Remove the trailing `if not entries:\n` from current file because it's incomplete
if current_content.strip().endswith('if not entries:'):
    current_content = current_content[:current_content.rfind('if not entries:')]

# Now modify ingest_batch inside the missing_parts to support the dual path
# The dual path logic:
new_ingest_batch = """
    def ingest_batch(
        self,
        tape_rows: List[Dict],
        dom_rows:  List[Dict],
        symbol:    str,
        batch_id:  Optional[str] = None,
        top_n:     int = 5,
    ) -> List[LiquidityCluster]:
        if not batch_id:
            batch_id = str(time.time_ns())

        is_sql_path = bool(tape_rows and "timestamp_ns" in tape_rows[0])

        if is_sql_path:
            clusters = self._build_clusters_sql(symbol, batch_id)
            tape = parse_tape_rows(tape_rows, symbol)
            dom  = parse_dom_rows(dom_rows, symbol)
            # Na fase SQL os dados ja estao inseridos no DuckDB via Arrow bulk_insert.
            # Entao pulamos a insercao repo.insert_tape_events etc.
        else:
            tape = parse_tape_rows(tape_rows, symbol)
            dom  = parse_dom_rows(dom_rows, symbol)

            if tape_rows and not tape:
                logging.warning("[ingest_batch] %d tape_rows sem parse", len(tape_rows))
                return []
            if not tape:
                return []

            dom_index = self._build_dom_index(dom_rows, self.cfg.tick_size, top_n=top_n)
            clusters = self._build_clusters_from_windows(tape, dom_index, symbol, batch_id)

            self.repo.begin()
            try:
                self.repo.insert_tape_events(tape)
                self.repo.insert_dom_levels(dom)
                self.repo.insert_clusters(clusters)
                self.repo.commit()
            except Exception:
                self.repo.rollback()
                raise

        snap = self.matrix.snapshot()
        try:
            self.matrix.build_from_events(tape, dom, clusters=clusters)

            original_signatures: dict[int, str] = {
                id(c): c.behavior_signature.value for c in clusters
            }
            current_batch_id = clusters[0].batch_id if clusters else None

            self._batch_counter = getattr(self, "_batch_counter", 0) + 1
            if self._batch_counter % 10 == 0:
                hotspots = self.matrix.hotspots(self.cfg.min_occurrences)
                for h in hotspots:
                    level_clusters = self.matrix.active_levels.get(h["price"], [])
                    refined = self.engine.post_classify(h["price"], level_clusters)
                    if current_batch_id:
                        for c in level_clusters:
                            if c.batch_id == current_batch_id:
                                c.behavior_signature = refined

            upgraded = [
                c for c in clusters
                if c.behavior_signature.value != original_signatures.get(id(c))
            ]
            if upgraded:
                self.repo.begin()
                try:
                    self.repo.conn.executemany(
                        "UPDATE liquidity_clusters SET behavior_signature = ? WHERE symbol = ? AND timestamp = ? AND price = ? AND batch_id = ?",
                        [
                            (c.behavior_signature.value, c.symbol, c.timestamp, c.price, c.batch_id)
                            for c in upgraded
                        ],
                    )
                    self.repo.commit()
                except Exception:
                    self.repo.rollback()
                    raise
        except Exception:
            self.matrix.restore(snap)
            raise

        if self.narrator is not None:
            self.narrator.invalidate_cache()

        return clusters
"""

# Replace the old ingest_batch with new_ingest_batch in missing_parts
import re
# Find the start of ingest_batch
match = re.search(r'    def ingest_batch\(', missing_parts)
if match:
    before = missing_parts[:match.start()]
    missing_parts = before + new_ingest_batch

with open('ingestion.py', 'w') as f:
    f.write(current_content + missing_parts)

print("Ingestion.py repaired successfully.")
