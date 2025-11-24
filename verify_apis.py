import requests
import json
import time

CONTROLLER_URL = "http://localhost:8080"

def test_put(key, value):
    print(f"Testing PUT key={key} value={value}...")
    try:
        response = requests.put(f"{CONTROLLER_URL}/put", json={"key": key, "value": value})
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"PUT failed: {e}")
        return False

def test_get(key):
    print(f"Testing GET key={key}...")
    try:
        response = requests.post(f"{CONTROLLER_URL}/get", json={"key": key})
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"GET failed: {e}")
        return False

if __name__ == "__main__":
    print("Starting API Verification...")
    
    # Test 1: Successful PUT
    if test_put("testKey1", "testValue1"):
        print("PUT Test Passed")
    else:
        print("PUT Test Failed")

    time.sleep(1)

    # Test 2: Successful GET
    if test_get("testKey1"):
        print("GET Test Passed")
    else:
        print("GET Test Failed")
