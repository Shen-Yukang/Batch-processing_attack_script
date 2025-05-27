def parse_config(config_path):
    """
    解析简单的key=value配置文件，支持#注释和字符串、数字类型。
    返回dict。
    """
    import os
    import re
    config = {}
    if not os.path.exists(config_path):
        return config
    with open(config_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            m = re.match(r'(\w+)\s*=\s*([^"\s][^\s]*|"[^"]*")', line)
            if m:
                key, value = m.group(1), m.group(2)
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.isdigit():
                    value = int(value)
                config[key] = value
    return config 