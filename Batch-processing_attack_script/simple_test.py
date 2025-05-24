#!/usr/bin/env python3
"""
简单的API测试脚本
"""

import openai
import sys

def test_models(api_key):
    """测试不同模型的可用性"""
    client = openai.OpenAI(api_key=api_key)
    
    models_to_test = [
        "gpt-4o-mini",
        "gpt-4o", 
        "gpt-4-turbo",
        "gpt-3.5-turbo"
    ]
    
    print("测试模型可用性...")
    print("=" * 50)
    
    for model in models_to_test:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=5
            )
            print(f"✅ {model}: 可用")
            print(f"   响应: {response.choices[0].message.content}")
        except Exception as e:
            print(f"❌ {model}: 不可用")
            print(f"   错误: {str(e)[:100]}...")
        print()

def main():
    if len(sys.argv) != 2:
        print("用法: python simple_test.py YOUR_API_KEY")
        return
    
    api_key = sys.argv[1]
    test_models(api_key)

if __name__ == "__main__":
    main()
