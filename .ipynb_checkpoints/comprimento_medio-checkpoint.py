import os
from subprocess import run
from pathlib import Path


folder_path = Path('carolina_output/')
files = [file for file in folder_path.glob('*.txt')]
files = sorted(files, key=lambda f: f.stat().st_size)

mean_size_arr = []
for file in files:
    total_symbols = 0
    with open(file, 'r', encoding='utf-8') as f:
        while True:
            symbols = f.read(1024)
            if not symbols:
                break
            total_symbols += len(symbols)
    
    run(['zip', '-9', f'{file}.zip', f'{file}'])

    file_bits = os.path.getsize(f'{file}.zip') * 8

    mean_size = file_bits / total_symbols
    mean_size_arr.append(mean_size) 

    print(f"[{file}] Quantidade de símbolos: {total_symbols} | Quantidade de bits pós compressão: {file_bits} | Comprimento médio: {mean_size:.4f}")
