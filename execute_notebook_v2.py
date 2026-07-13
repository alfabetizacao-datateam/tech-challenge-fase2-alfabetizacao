#!/usr/bin/env python3
"""
Execute Jupyter notebook cells preserving state using nbclient.
"""

import json
import sys
import os
from pathlib import Path
from nbclient import NotebookClient
import nbformat

def execute_notebook_with_output(notebook_path, output_file=None):
    """Execute notebook and save results with detailed output."""

    # Read notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    # Create client
    client = NotebookClient(nb, timeout=600, kernel_name='python3')

    # Execute notebook
    try:
        client.execute()
    except Exception as e:
        print(f"Error during execution: {e}")

    # Print results
    code_cell_idx = 0
    for cell_idx, cell in enumerate(nb.cells):
        if cell['cell_type'] == 'code':
            code_cell_idx += 1

            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

            # Skip empty cells
            if not source.strip():
                continue

            print(f"\n{'='*80}")
            print(f"CELL #{code_cell_idx}")
            print(f"{'='*80}")

            print(f"\n**Código:**")
            print(f"```python")
            print(source[:500] if len(source) > 500 else source)
            if len(source) > 500:
                print(f"... (truncado, total {len(source)} chars)")
            print("```")

            # Print outputs
            if 'outputs' in cell and cell['outputs']:
                print(f"\n**Output:**")
                for output in cell['outputs']:
                    if output['output_type'] == 'stream':
                        print(output['text'])
                    elif output['output_type'] == 'execute_result':
                        if 'text/plain' in output['data']:
                            print(output['data']['text/plain'])
                    elif output['output_type'] == 'display_data':
                        if 'text/plain' in output['data']:
                            print(output['data']['text/plain'])
                    elif output['output_type'] == 'error':
                        print(f"ERROR: {output['ename']}: {output['evalue']}")
            else:
                print(f"\n**Output:** (sem output)")


if __name__ == '__main__':
    notebook_name = sys.argv[1] if len(sys.argv) > 1 else '02_gold_marts_analytics.ipynb'
    notebook_path = Path(__file__).parent / 'notebooks' / notebook_name

    if not notebook_path.exists():
        print(f"Notebook not found: {notebook_path}")
        sys.exit(1)

    print(f"Executing notebook: {notebook_name}\n")
    execute_notebook_with_output(notebook_path)
