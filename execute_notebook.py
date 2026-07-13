#!/usr/bin/env python3
"""
Execute Jupyter notebook cells and capture output for documentation.
Focuses on code cells and their actual output.
"""

import json
import sys
import io
import traceback
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

def execute_notebook(notebook_path):
    """Execute notebook and print cell-by-cell results."""

    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)

    cells = notebook.get('cells', [])

    code_cell_count = 0
    for cell_idx, cell in enumerate(cells):
        if cell['cell_type'] == 'code':
            code_cell_count += 1
            source = ''.join(cell['source'])

            # Skip empty cells
            if not source.strip():
                continue

            print(f"\n{'='*80}")
            print(f"CELL #{code_cell_count}: Code Cell {cell_idx}")
            print(f"{'='*80}")

            print("\n**Código:**")
            print("```python")
            print(source)
            print("```")

            print("\n**Output:**")

            # Create execution namespace
            exec_namespace = {}

            # Capture stdout and stderr
            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()

            try:
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    exec(source, exec_namespace)

                output = stdout_capture.getvalue()
                if output:
                    print(output)
                else:
                    print("(Sem output direto)")

            except Exception as e:
                print(f"**ERRO:** {type(e).__name__}: {str(e)}")
                print("\n**Traceback:**")
                traceback.print_exc()

            stderr_out = stderr_capture.getvalue()
            if stderr_out:
                print(f"\n**Stderr:**\n{stderr_out}")

if __name__ == '__main__':
    notebook_name = sys.argv[1] if len(sys.argv) > 1 else '02_gold_marts_analytics.ipynb'
    notebook_path = Path(__file__).parent / 'notebooks' / notebook_name

    if not notebook_path.exists():
        print(f"Notebook not found: {notebook_path}")
        sys.exit(1)

    print(f"Executing notebook: {notebook_name}\n")
    execute_notebook(notebook_path)
