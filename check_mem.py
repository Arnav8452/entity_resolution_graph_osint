import psutil
with open('sys_mem.txt', 'w') as f:
    f.write(f'Virtual Memory: {psutil.virtual_memory()}\n')
    for proc in sorted(psutil.process_iter(['pid', 'name', 'memory_info']), key=lambda p: p.info['memory_info'].rss, reverse=True)[:10]:
        val = proc.info['memory_info'].rss / 1024 / 1024
        f.write(f"{proc.info['name']}: {val} MB\n")
