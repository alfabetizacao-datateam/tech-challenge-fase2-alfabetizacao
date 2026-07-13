#!/usr/bin/env python3
"""
Generate comprehensive notebook execution report.
"""

import json
import sys
import os
from pathlib import Path
from nbclient import NotebookClient
import nbformat
import re

def sanitize_output(text):
    """Remove ANSI codes and fix encoding issues."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', str(text))

def execute_and_report(notebook_path, output_file=None):
    """Execute notebook and generate markdown report."""

    # Read notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    print(f"[*] Executing {notebook_path.name}...")

    # Create client
    client = NotebookClient(nb, timeout=600, kernel_name='python3', allow_errors=True)

    # Execute notebook
    try:
        client.execute()
    except Exception as e:
        print(f"[!] Execution error (continuing): {type(e).__name__}")

    # Collect markdown
    md_lines = []
    md_lines.append(f"# Notebook: {notebook_path.stem}")
    md_lines.append("")

    code_cell_idx = 0
    for cell_idx, cell in enumerate(nb.cells):
        if cell['cell_type'] == 'markdown':
            # Include markdown cells
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            md_lines.append(source)
            md_lines.append("")

        elif cell['cell_type'] == 'code':
            code_cell_idx += 1
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

            # Skip empty cells
            if not source.strip():
                continue

            md_lines.append(f"## Célula #{code_cell_idx}")
            md_lines.append("")

            md_lines.append(f"**Código:**")
            md_lines.append("```python")
            # Truncate if too long
            if len(source) > 2000:
                md_lines.append(source[:2000])
                md_lines.append(f"... (truncado, {len(source)} chars totais)")
            else:
                md_lines.append(source)
            md_lines.append("```")
            md_lines.append("")

            # Print outputs
            if 'outputs' in cell and cell['outputs']:
                md_lines.append(f"**Output:**")
                md_lines.append("")
                for output in cell['outputs']:
                    if output['output_type'] == 'stream':
                        text = output['text']
                        # Truncate if too long
                        if len(text) > 3000:
                            md_lines.append("```")
                            md_lines.append(text[:3000])
                            md_lines.append(f"\n... (truncado, {len(text)} chars)")
                            md_lines.append("```")
                        else:
                            md_lines.append("```")
                            md_lines.append(text)
                            md_lines.append("```")
                    elif output['output_type'] == 'execute_result':
                        if 'text/plain' in output['data']:
                            text = output['data']['text/plain']
                            if len(text) > 3000:
                                md_lines.append("```")
                                md_lines.append(text[:3000])
                                md_lines.append(f"\n... (truncado, {len(text)} chars)")
                                md_lines.append("```")
                            else:
                                md_lines.append("```")
                                md_lines.append(text)
                                md_lines.append("```")
                    elif output['output_type'] == 'display_data':
                        if 'text/plain' in output['data']:
                            text = output['data']['text/plain']
                            md_lines.append("```")
                            md_lines.append(text)
                            md_lines.append("```")
                    elif output['output_type'] == 'error':
                        md_lines.append(f"```\nERRO: {output['ename']}: {output['evalue']}\n```")

                md_lines.append("")
            else:
                md_lines.append("*(Sem output direto)*")
                md_lines.append("")

    # Write report
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(md_lines))
        print(f"[OK] Report saved: {output_file}")
    else:
        print('\n'.join(md_lines))

    return md_lines


if __name__ == '__main__':
    notebook_name = sys.argv[1] if len(sys.argv) > 1 else '02_gold_marts_analytics.ipynb'
    notebooks_dir = Path(__file__).parent / 'notebooks'
    notebook_path = notebooks_dir / notebook_name
    output_file = Path(__file__).parent / f'NOTEBOOK_REPORT_{notebook_path.stem}.md'

    if not notebook_path.exists():
        print(f"Notebook not found: {notebook_path}")
        sys.exit(1)

    md_content = execute_and_report(notebook_path, output_file)
    print("\n[DONE] All complete!")
