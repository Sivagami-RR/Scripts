"""
coordination.py
───────────────
Master coordination script for tc_003 end-to-end flow.

Pre-conditions (done manually before running this script):
  - All CSVs uploaded
  - Magical stow completed
  - All batches reserved

What this script does in a loop:
  A) Allocate next 10 batches when ASSIGNED pick ops ≤ 500
  B) BACKGROUND: Induction thread runs continuously — drains ALL READY ORDER_INDUCTION ops regardless of batch
  C) Ensure batchless PICK flows on all PICK stations
  D) BACKGROUND: Drop-off thread runs continuously on all DROP stations
  Repeats until all batches are allocated AND all pick ops drain to 0.
"""

import csv
import queue
import time
import threading
import requests
import urllib3
import argparse
import sys
import os
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── Tee logger — mirrors all print() to both console and log file ────────────


class _Tee:
    """Write to both stdout and a log file simultaneously."""

    def __init__(self, filepath: str):
        self._file = open(filepath, "w", encoding="utf-8", buffering=1)
        self._stdout = sys.__stdout__

    def write(self, data):
        self._stdout.write(data)
        self._file.write(data)

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        self._file.close()


_tee_instance: _Tee | None = None
_log_path: str = ""


def setup_logging(log_path: str) -> None:
    global _tee_instance, _log_path
    _log_path = log_path
    _tee_instance = _Tee(log_path)
    sys.stdout = _tee_instance
    print(f"[log] Logging to: {log_path}")


def teardown_logging() -> None:
    if _tee_instance:
        sys.stdout = sys.__stdout__
        _tee_instance.close()


def write_induction_summary() -> None:
    """Write inducted orders summary as a separate file next to the main log."""
    summary_path = _log_path.replace(".log", "_inducted_orders.log")
    with _inducted_lock:
        orders = sorted(_inducted_orders)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Induction Summary — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total inducted orders: {len(orders)}\n")
        f.write("=" * 50 + "\n")
        for i, name in enumerate(orders, 1):
            f.write(f"{i:>4}. {name}\n")
    print(f"[log] Inducted orders summary written to: {summary_path}")


# ── Config ───────────────────────────────────────────────────────────────────

BASE_URL = "http://localhost"
WMS_URL = f"{BASE_URL}/wms-server/api/v1"
OWM_URL = f"{BASE_URL}/owm/api/v1"

AUTH_TOKEN = "autobootstrap"
HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

ALLOCATION_USER = "oks_tests"
INDUCTION_USER = "induct"
DROPOFF_USER = "drop_off"
ALLOCATION_TYPE = "PARTIAL"

BATCH_SIZE = 10  # batches per allocation wave
ASSIGNED_OP_THRESHOLD = 500  # only allocate next wave when ops ≤ this
POLL_INTERVAL = 30  # seconds between throttle checks in main loop
INDUCTION_POLL = 3  # seconds between retries when no READY induction op found
DROP_POLL = 5  # seconds between drop-off poll cycles

# ── Manual station overrides (optional) ─────────────────────────────────────
# Leave as None to auto-discover via capabilities API.
# Set to a list of station IDs if you want to explicitly control which station
# handles induction vs drop-off (useful when a station has both STOW+DROP caps).
#
# Example:
#   STOW_STATION_IDS_OVERRIDE = [101]   # this station → induction
#   DROP_STATION_IDS_OVERRIDE = [102]   # this station → drop-off

STOW_STATION_IDS_OVERRIDE = [101]  # e.g. [101]
DROP_STATION_IDS_OVERRIDE = [102]  # e.g. [102]

# ── Order induction data — loaded from CSV at startup ───────────────────────


def load_induction_orders(csv_path: str) -> list:
    """Return an ordered list of unique order IDs where 'Order requires induction' == TRUE."""
    seen = set()
    orders = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("Order requires induction", "").strip().upper() == "TRUE":
                order_id = row.get("Order ID", "").strip()
                if order_id and order_id not in seen:
                    seen.add(order_id)
                    orders.append(order_id)
    print(
        f"[bootstrap] Loaded {len(orders)} unique order(s) requiring induction from CSV."
    )
    return orders


def load_previously_inducted(log_path: str) -> set:
    """
    Read a previously written inducted orders log file and return the set of
    order names already inducted — used to safely resume after a mid-run stop.

    Expected log format:
        Induction Summary — 2026-05-13 15:55:10
        Total inducted orders: 5
        ==================================================
           1. tc_003_注文_083_000
           2. tc_003_注文_083_001
    """
    orders = set()
    if not os.path.exists(log_path):
        print(f"[resume] Log file not found: {log_path} — starting fresh.")
        return orders
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            # Lines with orders look like:  "   1. tc_003_注文_083_000"
            stripped = line.strip()
            if stripped and stripped[0].isdigit() and ". " in stripped:
                order_name = stripped.split(". ", 1)[1].strip()
                if order_name:
                    orders.add(order_name)
    print(
        f"[resume] Loaded {len(orders)} previously inducted order(s) from: {log_path}"
    )
    print(f"[resume] These orders will be skipped in this run.")
    return orders


# Tracks orders already successfully inducted  {order_id}
_inducted_orders: set = set()
_inducted_lock = threading.Lock()


# ── Batch discovery — fetched from API at startup, not hardcoded ────────────


def fetch_all_batch_fragments() -> list[dict]:
    """Paginate through only ASSIGNABLE/PARTIALLY_ASSIGNABLE orderBatchFragments."""
    fragments = []
    page = 1
    while True:
        data = get(
            f"{WMS_URL}/orderBatchFragments",
            params=[
                ("batchStatus", "ASSIGNABLE"),
                ("batchStatus", "PARTIALLY_ASSIGNABLE"),
                ("page", page),
                ("size", 100),
            ],
        )
        items = data.get("items", [])
        fragments.extend(items)
        if len(items) < 100:
            break
        page += 1
    print(
        f"[bootstrap] Fetched {len(fragments)} allocatable batch fragment(s) from API."
    )
    return fragments


# ── Generic HTTP helpers ─────────────────────────────────────────────────────

NETWORK_RETRIES = 3  # max retries on connection errors
NETWORK_RETRY_WAIT = 10  # seconds to wait between retries


def get(url, params=None):
    for attempt in range(1, NETWORK_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, verify=False)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ConnectionError as e:
            print(
                f"  [network] GET connection error (attempt {attempt}/{NETWORK_RETRIES}): {e}"
            )
            if attempt < NETWORK_RETRIES:
                print(f"  [network] Retrying in {NETWORK_RETRY_WAIT}s …")
                time.sleep(NETWORK_RETRY_WAIT)
            else:
                print(f"  [network] All {NETWORK_RETRIES} retries exhausted. Raising.")
                raise


def post(url, body=None):
    for attempt in range(1, NETWORK_RETRIES + 1):
        try:
            r = requests.post(url, headers=HEADERS, json=body or {}, verify=False)
            return r
        except requests.exceptions.ConnectionError as e:
            print(
                f"  [network] POST connection error (attempt {attempt}/{NETWORK_RETRIES}): {e}"
            )
            if attempt < NETWORK_RETRIES:
                print(f"  [network] Retrying in {NETWORK_RETRY_WAIT}s …")
                time.sleep(NETWORK_RETRY_WAIT)
            else:
                print(f"  [network] All {NETWORK_RETRIES} retries exhausted. Raising.")
                raise


# ── Flow teardown helper ─────────────────────────────────────────────────────


def stop_flows(kind: str, station_ids: list, label: str) -> None:
    """Fetch all ASSIGNED flows of `kind` on given stations and finalize them as CANCELED."""
    if not station_ids:
        return
    print(
        f"  [{label}] Finalizing all ASSIGNED {kind} flows on stations {station_ids} …"
    )
    params = [("kind", kind), ("status", "ASSIGNED"), ("page", 1), ("size", 100)] + [
        ("stationIds", sid) for sid in station_ids
    ]
    try:
        flows = get(f"{OWM_URL}/flows", params=params).get("items", [])
    except Exception as e:
        print(f"  [{label}] Could not fetch {kind} flows to finalize: {e}")
        return
    if not flows:
        print(f"  [{label}] No active {kind} flows found — nothing to finalize.")
        return
    for flow in flows:
        flow_id = flow["id"]
        try:
            r = post(f"{OWM_URL}/flows/{flow_id}/finalize", {"status": "CANCELED"})
            if r.status_code in (200, 204):
                print(
                    f"  [{label}] ✓ Finalized {kind} flow {flow_id} (station {flow['station']['id']})"
                )
            else:
                print(
                    f"  [{label}] ✗ Could not finalize flow {flow_id} — {r.status_code}: {r.text}"
                )
        except Exception as e:
            print(f"  [{label}] Exception finalizing flow {flow_id}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — BATCH ALLOCATION
# ═══════════════════════════════════════════════════════════════════════════════


def get_mhe_id() -> str:
    items = get(
        f"{WMS_URL}/materialHandlingEquipments",
        {"type": "RR_ASRS", "page": 1, "size": 100},
    ).get("items", [])
    if not items:
        raise RuntimeError("No RR_ASRS MHE found.")
    mhe = items[0]
    print(f"[bootstrap] MHE: {mhe['name']}  (id={mhe['id']})")
    return mhe["id"]


def get_batch_fragment(batch_name: str) -> dict | None:
    """Return the full batch fragment dict (including status) for the given name."""
    items = get(
        f"{WMS_URL}/orderBatchFragments", {"name": batch_name, "page": 1, "size": 10}
    ).get("items", [])
    return next((i for i in items if i.get("name") == batch_name), None)


# Only attempt allocation when the batch is in one of these statuses
_ALLOCATABLE_STATUSES = {"ASSIGNABLE", "PARTIALLY_ASSIGNABLE"}


def allocate_batch(batch_id: str, mhe_id: str) -> bool:
    body = {
        "user": ALLOCATION_USER,
        "materialHandlingEquipment": {"id": mhe_id},
        "type": ALLOCATION_TYPE,
    }
    r = post(f"{WMS_URL}/orderBatchFragments/{batch_id}/allocate", body)
    if r.status_code in (200, 201, 202, 204):
        return True
    print(f"  [warn] Allocate failed for {batch_id} — {r.status_code}: {r.text}")
    return False


def get_assigned_pick_count() -> int:
    data = get(
        f"{OWM_URL}/operations",
        {
            "kind": "PICK",
            "status": "ASSIGNED",
            "page": 1,
            "size": 1,
        },
    )
    return data.get("total", 0)


def allocate_wave(batch_names: list, mhe_id: str) -> int:
    success = 0
    for name in batch_names:
        print(f"  ├─ '{name}' …", end=" ", flush=True)
        fragment = get_batch_fragment(name)
        if not fragment:
            print("✗ not found in WMS")
            continue
        if allocate_batch(fragment["id"], mhe_id):
            # Wait 5s then confirm status has changed
            time.sleep(5)
            confirmed = get_batch_fragment(name)
            confirmed_status = (
                confirmed.get("status", "UNKNOWN") if confirmed else "NOT_FOUND"
            )
            if confirmed_status not in _ALLOCATABLE_STATUSES:
                print(f"✓ confirmed (status={confirmed_status})")
            else:
                print(
                    f"⚠ API said OK but status still {confirmed_status} — may need re-queue"
                )
            success += 1
        else:
            print("✗")
    return success


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — ORDER INDUCTION  (scoped to the allocated wave's batches)
#              Runs in a background thread so main loop is never blocked
# ═══════════════════════════════════════════════════════════════════════════════

_induction_thread: object = None
_induction_stop = threading.Event()


def get_stations_by_capability(capability: str) -> list:
    return get(
        f"{OWM_URL}/stations", {"capabilities": capability, "page": 1, "size": 100}
    ).get("items", [])


def ensure_flow(station_id: int, kind: str, user: str, metadata: dict = {}) -> None:
    """Create a flow of the given kind for the station if one isn't already ASSIGNED."""
    existing = get(
        f"{OWM_URL}/flows",
        {
            "kind": kind,
            "status": "ASSIGNED",
            "stationIds": station_id,
            "page": 1,
            "size": 10,
        },
    ).get("items", [])
    if existing:
        return
    body = {
        "kind": kind,
        "station": {"id": station_id},
        "user": user,
        "metadata": metadata,
    }
    r = post(f"{OWM_URL}/flows", body)
    if r.status_code in (200, 201):
        print(f"  [flow] Created {kind} flow for station {station_id}.")
    else:
        print(
            f"  [flow] Failed to create {kind} flow for station {station_id} — {r.status_code}: {r.text}"
        )


def ensure_induction_flows(stow_station_ids: list) -> None:
    for sid in stow_station_ids:
        ensure_flow(sid, "ORDER_INDUCTION", INDUCTION_USER)


def set_op_in_progress(op_id: int) -> bool:
    r = post(f"{OWM_URL}/operations/{op_id}/setInProgress")
    return r.status_code in (200, 204)


def get_bin(bin_id: int) -> dict:
    return get(f"{OWM_URL}/bins/{bin_id}")


def link_inducted_container(mhe_id: str, container_id: str, order_name: str) -> bool:
    url = f"{WMS_URL}/materialHandlingEquipments/{mhe_id}/actions/linkInductedContainerPartition"
    body = {
        "containerPartition": {"partitionIndex": 1, "container": {"id": container_id}},
        "orderDetails": {"name": order_name},
        "markPartitionFull": True,
    }
    r = post(url, body)
    if r.status_code in (200, 201, 204):
        print(
            f"  [induction] Container {container_id} linked for order '{order_name}'."
        )
        return True
    print(
        f"  [induction] linkInductedContainerPartition failed — {r.status_code}: {r.text}"
    )
    return False


def _induction_loop(
    mhe_id: str, stow_station_ids: list, induction_queue: queue.Queue, total_orders: int
) -> None:
    """Continuous background induction thread — processes ALL READY ORDER_INDUCTION ops.
    Pops the next order name from the CSV queue when a bin is ready at the station.
    Tracks inducted orders globally to avoid duplicates (1 order : 1 bin).
    """
    print(
        f"[induction] Background thread started. Orders to induct: {induction_queue.qsize()}"
    )
    ok = skip = 0
    while not _induction_stop.is_set():
        try:
            # Ensure ORDER_INDUCTION flows are alive on all STOW stations
            ensure_induction_flows(stow_station_ids)

            # Fetch all READY induction ops — not scoped to any batch
            ops = get(
                f"{OWM_URL}/operations",
                {
                    "kind": "ORDER_INDUCTION",
                    "status": "READY",
                    "page": 1,
                    "size": 100,
                },
            ).get("items", [])

            if not ops:
                print(f"  [induction] No READY ops — waiting {INDUCTION_POLL}s …")
                _induction_stop.wait(INDUCTION_POLL)
                if _induction_stop.is_set():
                    break
                continue

            for op in ops:
                if _induction_stop.is_set():
                    break
                op_id = op["id"]
                print(f"\n  [induction] Processing op {op_id} …")

                # Fetch bin details using direct endpoint GET /bins/{id}
                try:
                    full_op = get(f"{OWM_URL}/operations/{op_id}")
                    bin_id = full_op["sourceBin"]["id"]
                    bin_data = get_bin(bin_id)
                    container_id = bin_data.get("containerId")
                except Exception as e:
                    print(f"  [induction] Could not fetch op/bin details: {e}")
                    skip += 1
                    continue

                if not container_id:
                    print(f"  [induction] No containerId for op {op_id} — skipping.")
                    skip += 1
                    continue

                container_barcode = bin_data.get("containerBarcode", "")
                print(
                    f"  [induction] Bin {bin_id} → container={container_id}  barcode={container_barcode}"
                )

                # Pop next order name from CSV queue
                try:
                    order_name = induction_queue.get_nowait()
                except queue.Empty:
                    print(f"  [induction] No more orders in queue — all inducted!")
                    print(
                        f"  [induction] Finalizing ORDER_INDUCTION flows on stations {stow_station_ids} …"
                    )
                    stop_flows("ORDER_INDUCTION", stow_station_ids, "induction")
                    _induction_stop.set()
                    break

                if not set_op_in_progress(op_id):
                    print(
                        f"  [induction] Could not set op {op_id} IN_PROGRESS — re-queuing order '{order_name}'."
                    )
                    induction_queue.put(order_name)
                    skip += 1
                    continue

                print(
                    f"  [induction] Inducting order '{order_name}' (container={container_id})"
                )

                try:
                    if not link_inducted_container(mhe_id, container_id, order_name):
                        induction_queue.put(order_name)
                        skip += 1
                        continue
                except Exception as e:
                    print(f"  [induction] link exception: {e}")
                    induction_queue.put(order_name)
                    skip += 1
                    continue

                try:
                    r = post(
                        f"{OWM_URL}/operations/{op_id}/logResults",
                        {"result": "SUCCESS", "metadata": None, "isFinal": True},
                    )
                    print(f"  [induction] op {op_id} logResults → {r.status_code}")
                    # Mark order as inducted and write summary immediately for resume
                    with _inducted_lock:
                        _inducted_orders.add(order_name)
                    write_induction_summary()
                    ok += 1
                    print(
                        f"  [induction] ✓ Inducted {ok}/{total_orders} orders so far."
                    )
                except Exception as e:
                    print(f"  [induction] logResults exception (non-fatal): {e}")
                    ok += 1

                time.sleep(5)  # 5s pause between each induction

        except Exception as e:
            print(f"  [induction] Error: {e}")
            _induction_stop.wait(INDUCTION_POLL)

    print(f"[induction] Background thread stopped — ✓ {ok} inducted  ✗ {skip} skipped.")
    with _inducted_lock:
        print(f"[induction] Total unique orders inducted: {len(_inducted_orders)}")


def start_induction_thread(
    mhe_id: str, stow_station_ids: list, induction_queue: queue.Queue, total_orders: int
) -> None:
    global _induction_thread, _induction_stop
    if _induction_thread and _induction_thread.is_alive():
        return
    _induction_stop.clear()
    _induction_thread = threading.Thread(
        target=_induction_loop,
        args=(mhe_id, stow_station_ids, induction_queue, total_orders),
        daemon=True,
        name="induction",
    )
    _induction_thread.start()


def stop_induction_thread() -> None:
    _induction_stop.set()
    if _induction_thread:
        _induction_thread.join(timeout=15)


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — BATCHLESS PICKING
# ═══════════════════════════════════════════════════════════════════════════════


def ensure_batchless_pick_flows() -> None:
    print("\n[picking] Ensuring batchless PICK flows on all PICK stations …")
    for s in get_stations_by_capability("PICK"):
        ensure_flow(
            s["id"],
            "PICK",
            ALLOCATION_USER,
            metadata={"externalOrderBatchFragmentId": None},
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — CONTINUOUS DROP-OFF  (background thread)
# ═══════════════════════════════════════════════════════════════════════════════

_drop_thread: object = None
_drop_stop = threading.Event()


def _ensure_drop_flows(station_ids: list) -> None:
    active = get(
        f"{OWM_URL}/flows",
        params=[("kind", "DROP"), ("status", "ASSIGNED"), ("page", 1), ("size", 100)]
        + [("stationIds", sid) for sid in station_ids],
    ).get("items", [])
    active_station_ids = {f["station"]["id"] for f in active}
    for sid in station_ids:
        if sid not in active_station_ids:
            body = {
                "kind": "DROP",
                "station": {"id": sid},
                "user": DROPOFF_USER,
                "metadata": {
                    "externalOrderBatchFragmentId": None,
                    "exceptionStatusFilter": "NONE",
                },
            }
            r = post(f"{OWM_URL}/flows", body)
            if r.status_code in (200, 201):
                print(f"  [dropoff] Created DROP flow for station {sid}.")
            # else: not ready yet (no picks done) — silently retry next cycle


def _drop_loop(station_ids: list) -> None:
    print(f"[dropoff] Background thread started for stations {station_ids}.")
    while not _drop_stop.is_set():
        try:
            _ensure_drop_flows(station_ids)

            ops = get(
                f"{OWM_URL}/operations",
                params=[
                    ("kind", "DROP"),
                    ("status", "READY"),
                    ("page", 1),
                    ("size", 100),
                ]
                + [("stationIds", sid) for sid in station_ids],
            ).get("items", [])

            for op in ops:
                op_id = op["id"]
                source_bin_id = op["sourceBin"]["id"]
                try:
                    bin_data = get(f"{OWM_URL}/bins/{source_bin_id}")
                    container_id = bin_data.get("containerId")
                    barcode = bin_data.get("containerBarcode", "?")
                except Exception as e:
                    print(f"  [dropoff] Could not fetch bin {source_bin_id}: {e}")
                    continue

                if not container_id:
                    print(
                        f"  [dropoff] No containerId for bin {source_bin_id} — skipping."
                    )
                    continue

                bin_role = bin_data.get("role")
                if bin_role != "ORDER":
                    print(
                        f"  [dropoff] SKU bin detected {barcode} (role={bin_role}) — canceling op {op_id}"
                    )
                    r = post(
                        f"{OWM_URL}/operations/{op_id}/finalize", {"status": "CANCELED"}
                    )
                    if r.status_code in (200, 204):
                        print(f"  [dropoff] ✓ Op {op_id} canceled successfully.")
                    else:
                        print(
                            f"  [dropoff] ✗ Failed to cancel op {op_id} — {r.status_code}: {r.text}"
                        )
                    continue

                print(
                    f"  [dropoff] Drop-off for {barcode} (op {op_id}) …",
                    end=" ",
                    flush=True,
                )
                set_op_in_progress(op_id)
                r = post(
                    f"{OWM_URL}/operations/{op_id}/logResults",
                    {"result": "SUCCESS", "metadata": {"type": "CONTENT_SHIPPING"}},
                )
                print(f"status={r.status_code}")

        except Exception as e:
            print(f"  [dropoff] Error: {e}")

        _drop_stop.wait(DROP_POLL)

    print("[dropoff] Background thread stopped.")


def start_dropoff_thread(station_ids: list) -> None:
    global _drop_thread, _drop_stop
    if _drop_thread and _drop_thread.is_alive():
        return
    _drop_stop.clear()
    _drop_thread = threading.Thread(
        target=_drop_loop, args=(station_ids,), daemon=True, name="dropoff"
    )
    _drop_thread.start()


def stop_dropoff_thread() -> None:
    _drop_stop.set()
    if _drop_thread:
        _drop_thread.join(timeout=15)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN COORDINATION LOOP
# ═══════════════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="Coordination Script — tc_003 end-to-end"
    )
    parser.add_argument(
        "--orders-csv",
        required=True,
        help="Path to the orders CSV file (e.g. /path/to/tc_003_orders.csv)",
    )
    parser.add_argument(
        "--log-dir",
        default=".",
        help="Directory to write log files (default: current directory)",
    )
    parser.add_argument(
        "--resume-log",
        default=None,
        help="Path to a previous inducted orders log file to resume from "
        "(e.g. logs/coordination_20260513_155233_inducted_orders.log). "
        "Orders already in this file will be skipped.",
    )
    args = parser.parse_args()

    # Set up logging — all print() output goes to console + log file
    os.makedirs(args.log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(args.log_dir, f"coordination_{timestamp}.log")
    setup_logging(log_path)

    print("=" * 65)
    print("Coordination Script — tc_003 end-to-end")
    print("Pre-conditions: upload ✓  magical-stow ✓  reserve ✓")
    print("=" * 65)

    mhe_id = get_mhe_id()

    # Load induction orders from CSV
    induction_orders = load_induction_orders(args.orders_csv)
    total_orders = len(induction_orders)

    # Pre-populate inducted orders from a previous run log (resume safety)
    if args.resume_log:
        previously_inducted = load_previously_inducted(args.resume_log)
        with _inducted_lock:
            _inducted_orders.update(previously_inducted)
        print(
            f"[resume] {len(previously_inducted)} order(s) pre-loaded — will not be re-inducted."
        )
    else:
        print("[resume] No resume log provided — starting fresh.")

    # Build induction queue — ordered list from CSV, minus already inducted (resume)
    with _inducted_lock:
        already_done = set(_inducted_orders)
    remaining_orders = [o for o in induction_orders if o not in already_done]
    induction_queue = queue.Queue()
    for o in remaining_orders:
        induction_queue.put(o)
    print(f"[bootstrap] Induction queue: {induction_queue.qsize()} order(s) remaining.")

    # Discover station IDs by capability (or use manual override if set)
    pick_stations = [s["id"] for s in get_stations_by_capability("PICK")]
    drop_stations = DROP_STATION_IDS_OVERRIDE or [
        s["id"] for s in get_stations_by_capability("DROP")
    ]
    stow_stations = STOW_STATION_IDS_OVERRIDE or [
        s["id"] for s in get_stations_by_capability("STOW")
    ]
    print(
        f"[bootstrap] PICK stations : {pick_stations} {'(manual override)' if not STOW_STATION_IDS_OVERRIDE else ''}"
    )
    print(
        f"[bootstrap] DROP stations : {drop_stations} {'(manual override)' if DROP_STATION_IDS_OVERRIDE else '(auto-discovered)'}"
    )
    print(
        f"[bootstrap] STOW stations : {stow_stations} {'(manual override)' if STOW_STATION_IDS_OVERRIDE else '(auto-discovered)'}"
    )

    # ── Step 1: Fetch only allocatable batches directly from API ────────────
    all_fragments = fetch_all_batch_fragments()
    all_names = [f["name"] for f in all_fragments]
    total = len(all_names)

    remaining = list(all_names)
    wave_num = 0
    print(
        f"[coordinator] {len(remaining)} allocatable batch(es) found. Starting loop …"
    )

    # Start induction + drop-off background threads at bootstrap
    # Explicitly ensure ORDER_INDUCTION flows are live before starting thread
    # (covers resume case where Ctrl+C previously cancelled all flows)
    ensure_induction_flows(stow_stations)
    start_induction_thread(mhe_id, stow_stations, induction_queue, total_orders)
    start_dropoff_thread(drop_stations)

    try:
        # ── Phase 1: Allocate all remaining batches in waves ─────────────────
        while remaining:
            ops = get_assigned_pick_count()
            print(
                f"\n[coordinator] Assigned pick ops: {ops}  |  Still queued: {len(remaining)}/{total}"
            )

            if ops <= ASSIGNED_OP_THRESHOLD:
                wave_num += 1
                wave = remaining[:BATCH_SIZE]
                remaining = remaining[BATCH_SIZE:]

                print(f"\n{'─' * 65}")
                print(
                    f"[wave {wave_num}] Allocating {len(wave)} batch(es): {wave[0]} … {wave[-1]}"
                )
                ok = allocate_wave(wave, mhe_id)
                print(
                    f"[wave {wave_num}] {ok}/{len(wave)} allocated (or already done)."
                )

                # ── B: Induction runs continuously in background thread ────
                # ── C: Ensure batchless PICK flows ────────────────────────
                ensure_batchless_pick_flows()

                # ── D: Drop-off runs continuously in background thread ────

            else:
                print(
                    f"[coordinator] Ops > {ASSIGNED_OP_THRESHOLD} — waiting {POLL_INTERVAL}s …"
                )
                time.sleep(POLL_INTERVAL)

        # ── Phase 2: Verify — re-queue any that are still ASSIGNABLE ────────
        while True:
            print(
                f"\n[coordinator] Verifying allocation status for all {total} batches …"
            )
            still_pending = []
            for name in all_names:
                fragment = get_batch_fragment(name)
                status = fragment.get("status", "UNKNOWN") if fragment else "NOT_FOUND"
                if status in _ALLOCATABLE_STATUSES:
                    still_pending.append(name)
                    print(f"  ↻ '{name}' status={status} — re-queuing")

            if not still_pending:
                print(f"[coordinator] ✓ All {total} batches confirmed allocated.")
                break

            print(
                f"\n[coordinator] {len(still_pending)} batch(es) still need allocation — re-running allocation loop …"
            )
            remaining = still_pending
            while remaining:
                ops = get_assigned_pick_count()
                print(
                    f"\n[coordinator] Assigned pick ops: {ops}  |  Re-queued: {len(remaining)}"
                )
                if ops <= ASSIGNED_OP_THRESHOLD:
                    wave_num += 1
                    wave = remaining[:BATCH_SIZE]
                    remaining = remaining[BATCH_SIZE:]
                    print(
                        f"[wave {wave_num}] Re-allocating {len(wave)} batch(es): {wave[0]} … {wave[-1]}"
                    )
                    ok = allocate_wave(wave, mhe_id)
                    print(f"[wave {wave_num}] {ok}/{len(wave)} allocated.")
                    ensure_batchless_pick_flows()
                else:
                    print(
                        f"[coordinator] Ops > {ASSIGNED_OP_THRESHOLD} — waiting {POLL_INTERVAL}s …"
                    )
                    time.sleep(POLL_INTERVAL)
            # loop back to verify again

        # ── Phase 3: Wait for all pick ops to drain to zero ─────────────────
        print(f"\n[coordinator] Waiting for pick ops to drain …")
        while True:
            ops = get_assigned_pick_count()
            if ops == 0:
                print("[coordinator] ✓ All pick ops completed.")
                break
            print(
                f"[coordinator] {ops} pick ops remaining — waiting {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)

        # ── Phase 4: Wait for all drop-off ops to drain to zero ─────────────
        print(f"\n[coordinator] Waiting for drop-off ops to drain …")
        while True:
            drop_ops = get(
                f"{OWM_URL}/operations",
                {"kind": "DROP", "status": "READY", "page": 1, "size": 1},
            ).get("total", 0)
            if drop_ops == 0:
                print("[coordinator] ✓ All drop-off ops completed.")
                break
            print(
                f"[coordinator] {drop_ops} drop-off op(s) remaining — waiting {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)

        # ── Phase 5: Keep drop-off running — wait for manual Ctrl+C ─────────
        print("\n[coordinator] All ops done. Drop-off thread still running.")
        print("[coordinator] Press Ctrl+C to stop drop-off and exit.")
        while True:
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[coordinator] Interrupted by user.")
        print("[coordinator] Stopping induction and drop-off flows …")
        stop_flows("ORDER_INDUCTION", stow_stations, "induction")
        stop_flows("DROP", drop_stations, "dropoff")

    finally:
        print("[coordinator] Stopping induction thread …")
        stop_induction_thread()
        print("[coordinator] Stopping drop-off thread …")
        stop_dropoff_thread()
        print("[coordinator] Exited cleanly.")
        write_induction_summary()
        teardown_logging()


if __name__ == "__main__":
    main()
