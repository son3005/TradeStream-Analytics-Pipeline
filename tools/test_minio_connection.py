import urllib.request
import urllib.error
import sys

def check_endpoint(name, url):
    print(f"[*] Checking {name} at {url}...")
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as response:
            status = response.getcode()
            if status == 200:
                print(f"[+] {name} is ONLINE and healthy! (HTTP {status})")
                return True
            else:
                print(f"[!] {name} returned status code: {status}")
                return False
    except urllib.error.URLError as e:
        print(f"[X] {name} is OFFLINE or unreachable. Error: {e.reason}")
        return False
    except Exception as e:
        print(f"[X] Unexpected error checking {name}: {e}")
        return False

def main():
    print("=" * 50)
    print("      MINIO CONNECTION & HEALTH CHECK")
    print("=" * 50)
    
    api_url = "http://localhost:9000/minio/health/live"
    console_url = "http://localhost:9001"
    
    api_ok = check_endpoint("MinIO API (S3 Endpoint)", api_url)
    console_ok = check_endpoint("MinIO Console UI", console_url)
    
    print("-" * 50)
    if api_ok and console_ok:
        print("[SUCCESS] MinIO storage service is completely ready for Phase 3!")
        sys.exit(0)
    else:
        print("[FAILED] Some MinIO services are not reachable yet. Please ensure containers are running.")
        sys.exit(1)

if __name__ == "__main__":
    main()
