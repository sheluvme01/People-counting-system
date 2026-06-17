#!/usr/bin/env python3
"""
E2E Test — People Counting System
==================================
1. Gửi N khung hình tổng hợp qua camera-server
2. Đợi kết quả xử lý
3. Truy vấn storage-server và in báo cáo
"""

import sys
import time
import json
import base64
import requests
import numpy as np
import cv2

CAMERA_URL  = "http://localhost:5001"
PROCESS_URL = "http://localhost:5002"
STORAGE_URL = "http://localhost:5003"

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"


def banner(msg):
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}  {msg}{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")


def ok(msg):  print(f"{GREEN}✅ {msg}{RESET}")
def err(msg): print(f"{RED}❌ {msg}{RESET}")
def info(msg):print(f"{YELLOW}ℹ  {msg}{RESET}")


def wait_for_service(url: str, name: str, retries=30):
    for i in range(retries):
        try:
            r = requests.get(f"{url}/health", timeout=3)
            if r.status_code == 200:
                ok(f"{name} is up")
                return True
        except Exception:
            pass
        print(f"  Waiting for {name} ({i+1}/{retries})…", end='\r')
        time.sleep(3)
    err(f"{name} did not start")
    return False


def generate_frame(n_people: int, frame_idx: int) -> bytes:
    """Synthetic 640×480 frame with n_people rectangles."""
    img = np.zeros((480, 640, 3), np.uint8)
    img[:] = (25, 25, 35)
    for k in range(n_people):
        x1 = np.random.randint(20, 540)
        y1 = np.random.randint(20, 330)
        x2 = x1 + np.random.randint(50, 90)
        y2 = y1 + np.random.randint(100, 160)
        color = tuple(int(c) for c in np.random.randint(120, 240, 3).tolist())
        cv2.rectangle(img, (x1, y1), (min(x2,639), min(y2,479)), color, -1)
        cv2.putText(img, f"P{k+1}", (x1+5, y1+20),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255,255,255), 1)
    cv2.putText(img, f"Frame {frame_idx}", (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255,255,255), 2)
    _, buf = cv2.imencode('.jpg', img)
    return buf.tobytes()


def test_direct_detection():
    banner("Test 1: Direct detection (no Kafka)")
    frame = generate_frame(n_people=3, frame_idx=0)
    files = {'image': ('frame.jpg', frame, 'image/jpeg')}
    try:
        r = requests.post(f"{PROCESS_URL}/detect", files=files, timeout=30)
        d = r.json()
        ok(f"People detected: {d['people_count']}  |  time: {d['processing_ms']} ms")
        info(f"Detector: {d['detector']}")
        for b in d['bounding_boxes']:
            info(f"  Box: x={b['x']} y={b['y']} w={b['width']} h={b['height']} "
                 f"conf={b['confidence']}")
    except Exception as exc:
        err(f"Direct detection failed: {exc}")


def test_full_pipeline(camera_id="CAM-TEST-001", n_frames=5):
    banner(f"Test 2: Full Kafka pipeline  |  camera={camera_id}  frames={n_frames}")

    # Trigger simulation
    payload = {'camera_id': camera_id, 'num_frames': n_frames}
    try:
        r = requests.post(f"{CAMERA_URL}/simulate-camera", json=payload, timeout=15)
        ok(f"Simulation started: {r.json()}")
    except Exception as exc:
        err(f"Cannot start simulation: {exc}")
        return

    # Wait for processing + storage
    info(f"Waiting {n_frames * 2 + 5}s for pipeline to complete…")
    time.sleep(n_frames * 2 + 5)


def test_storage_queries(camera_id="CAM-TEST-001"):
    banner("Test 3: Storage server queries")

    # /results
    try:
        r = requests.get(f"{STORAGE_URL}/results",
                         params={'camera_id': camera_id, 'limit': 10}, timeout=10)
        d = r.json()
        ok(f"/results → total={d['total']}  returned={len(d['results'])}")
        for rec in d['results'][:3]:
            info(f"  frame={rec.get('frame_id','?')[:8]}…  "
                 f"people={rec.get('people_count','?')}  "
                 f"proc={rec.get('processing_ms','?')}ms")
    except Exception as exc:
        err(f"/results failed: {exc}")

    # /summary
    try:
        r = requests.get(f"{STORAGE_URL}/summary", timeout=10)
        d = r.json()
        ok(f"/summary → {d['total_cameras']} camera(s)")
        for cam in d['cameras']:
            info(f"  {cam['camera_id']}: frames={cam['total_frames']}  "
                 f"total_people={cam['total_people']}  avg={cam['avg_people']}")
    except Exception as exc:
        err(f"/summary failed: {exc}")

    # /timeline
    try:
        r = requests.get(f"{STORAGE_URL}/timeline",
                         params={'camera_id': camera_id}, timeout=10)
        d = r.json()
        ok(f"/timeline → {len(d['timeline'])} buckets")
    except Exception as exc:
        err(f"/timeline failed: {exc}")


def test_camera_upload():
    banner("Test 4: Direct frame upload via camera-server")
    frame = generate_frame(n_people=4, frame_idx=99)
    files = {'frame': ('frame.jpg', frame, 'image/jpeg')}
    data  = {'camera_id': 'CAM-UPLOAD-001'}
    try:
        r = requests.post(f"{CAMERA_URL}/send-frame",
                          files=files, data=data, timeout=15)
        ok(f"Upload response: {r.json()}")
    except Exception as exc:
        err(f"Upload failed: {exc}")


def main():
    print(f"\n{BLUE}🚀 People Counting System — E2E Test Suite{RESET}\n")

    # Health checks
    banner("Health Checks")
    services_ok = all([
        wait_for_service(CAMERA_URL,  "Camera Server"),
        wait_for_service(PROCESS_URL, "Processing Server"),
        wait_for_service(STORAGE_URL, "Storage Server"),
    ])

    if not services_ok:
        err("One or more services are not available. Abort.")
        sys.exit(1)

    test_direct_detection()
    test_camera_upload()
    test_full_pipeline(n_frames=5)
    test_storage_queries()

    banner("Test Complete 🎉")
    ok("All tests finished — check logs above for details")


if __name__ == '__main__':
    main()
