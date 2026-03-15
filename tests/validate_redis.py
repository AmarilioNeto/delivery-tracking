"""
Simple validation script that checks Redis for driver state keys and prints a few samples.
"""
import redis
import json

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    keys = r.keys('driver:*')
    print(f'Found {len(keys)} driver keys (sample up to 10):')
    for k in keys[:10]:
        v = r.get(k)
        try:
            j = json.loads(v)
        except Exception:
            j = v.decode('utf-8') if v else None
        print(k.decode('utf-8'), '->', j)

if __name__ == '__main__':
    main()
