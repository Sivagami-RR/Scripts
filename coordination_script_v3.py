"""
coordination_v3.py
──────────────────
Master coordination script for end-to-end flow.

Pre-conditions (done manually before running this script):
  - All CSVs uploaded
  - Magical stow completed
  - All batches reserved

What this script does:
  1. Parses order CSV → splits into waves of WAVE_SIZE batches (CSV priority order)
  2. Resolves batch names → fragment IDs from WMS API (once at startup)
  3. Main thread: allocates one wave at a time when assigned pick ops <= threshold
       - Single POST per wave with all batch fragment IDs
       - Strict-polls each batch until confirmed allocated
       - Stuck batches tracked → retried after all waves done
       - Pushes confirmed wave's orders into induction queue
  4. Background thread — Order Induction:
       - Blocks on queue until orders available (natural wave gating)
       - Processes READY ORDER_INDUCTION ops, links container → order
       - Finalizes induction flow when all orders inducted
  5. Background thread — Picking:
       - Periodically ensures batchless PICK flows on all PICK stations
  6. Background thread — Drop-off:
       - Periodically ensures batchless DROP flows on all DROP stations
       - Processes READY DROP ops (ORDER→SUCCESS, SKU→CANCEL)
"""

import csv
import queue
import time
import threading
import logging
import requests
import urllib3
import argparse
import sys
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── Logger ───────────────────────────────────────────────────────────────────

log = logging.getLogger("coordination")
_log_path: str = ""


def setup_logging(log_dir: str, timestamp: str) -> None:
    """Configure logger to write to both console and a timestamped log file."""
    global _log_path
    os.makedirs(log_dir, exist_ok=True)
    _log_path = os.path.join(log_dir, f"coordination_{timestamp}.log")

    fmt = logging.Formatter("%(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    fh = logging.FileHandler(_log_path, encoding="utf-8")
    fh.setFormatter(fmt)

    log.setLevel(logging.INFO)
    log.addHandler(sh)
    log.addHandler(fh)

    log.info(f"[log] Logging to: {_log_path}")


def teardown_logging() -> None:
    for handler in log.handlers[:]:
        handler.close()
        log.removeHandler(handler)


def write_induction_summary() -> None:
    """Write inducted orders summary as a separate file next to the main log."""
    if not _log_path:
        return
    summary_path = _log_path.replace(".log", "_inducted_orders.log")
    with _inducted_lock:
        orders = sorted(_inducted_orders)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Induction Summary — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total inducted orders: {len(orders)}\n")
        f.write("=" * 50 + "\n")
        for i, name in enumerate(orders, 1):
            f.write(f"{i:>4}. {name}\n")
    log.info(f"[log] Inducted orders summary written to: {summary_path}")


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

ASSIGNED_OP_THRESHOLD = 100   # allocate next wave when pick ops <= this
POLL_INTERVAL = 30            # seconds between threshold checks in main loop
INDUCTION_POLL = 3            # seconds queue.get() timeout (allows stop-event check)
DROP_POLL = 5                 # seconds between drop-off cycles
PICK_POLL = 10                # seconds between pick flow ensure cycles
ALLOC_POLL_RETRIES = 10       # max polls per batch after allocation POST
ALLOC_POLL_INTERVAL = 5       # seconds between each allocation poll attempt

NETWORK_RETRIES = 3
NETWORK_RETRY_WAIT = 10

# ── Manual station overrides (set to None to auto-discover) ──────────────────
# Needed when a single station has both STOW and DROP capabilities.
STOW_STATION_IDS_OVERRIDE: Optional[list] = [101]
DROP_STATION_IDS_OVERRIDE: Optional[list] = [102]

# ── Batch statuses ───────────────────────────────────────────────────────────
_ALLOCATABLE_STATUSES = {"ASSIGNABLE", "PARTIALLY_ASSIGNABLE"}          # not yet allocated
_PENDING_STATUSES     = {"ASSIGNABLE", "PARTIALLY_ASSIGNABLE"}          # keep polling
_FAILED_STATUSES      = {"CANCELLED", "EXCEPTION", "NEW"}               # not reserved / failed — skip


# ── Wave data structure ───────────────────────────────────────────────────────

@dataclass
class Wave:
    wave_num: int
    batch_names: list = field(default_factory=list)
    batch_fragment_ids: list = field(default_factory=list)
    orders: list = field(default_factory=list)   # induction order names, CSV priority


# ── Inducted orders tracking ─────────────────────────────────────────────────

_inducted_orders: set = set()
_inducted_lock = threading.Lock()

# ── Total orders to induct (set at startup) ───────────────────────────────────
_total_orders: int = 0


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def get(url, params=None):
    for attempt in range(1, NETWORK_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, verify=False)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ConnectionError as e:
            log.info(f"  [network] GET connection error (attempt {attempt}/{NETWORK_RETRIES}): {e}")
            if attempt < NETWORK_RETRIES:
                log.info(f"  [network] Retrying in {NETWORK_RETRY_WAIT}s …")
                time.sleep(NETWORK_RETRY_WAIT)
            else:
                log.info(f"  [network] All {NETWORK_RETRIES} retries exhausted. Raising.")
                raise


def post(url, body=None):
    for attempt in range(1, NETWORK_RETRIES + 1):
        try:
            r = requests.post(url, headers=HEADERS, json=body or {}, verify=False)
            return r
        except requests.exceptions.ConnectionError as e:
            log.info(f"  [network] POST connection error (attempt {attempt}/{NETWORK_RETRIES}): {e}")
            if attempt < NETWORK_RETRIES:
                log.info(f"  [network] Retrying in {NETWORK_RETRY_WAIT}s …")
                time.sleep(NETWORK_RETRY_WAIT)
            else:
                log.info(f"  [network] All {NETWORK_RETRIES} retries exhausted. Raising.")
                raise


# ── Flow teardown helper ──────────────────────────────────────────────────────

def stop_flows(kind: str, station_ids: list, label: str) -> None:
    """Fetch all ASSIGNED flows of `kind` on given stations and finalize them as CANCELED."""
    if not station_ids:
        return
    log.info(f"  [{label}] Finalizing all ASSIGNED {kind} flows on stations {station_ids} …")
    params = [("kind", kind), ("status", "ASSIGNED"), ("page", 1), ("size", 100)] + [
        ("stationIds", sid) for sid in station_ids
    ]
    try:
        flows = get(f"{OWM_URL}/flows", params=params).get("items", [])
    except Exception as e:
        log.info(f"  [{label}] Could not fetch {kind} flows to finalize: {e}")
        return
    if not flows:
        log.info(f"  [{label}] No active {kind} flows found — nothing to finalize.")
        return
    for flow in flows:
        flow_id = flow["id"]
        try:
            r = post(f"{OWM_URL}/flows/{flow_id}/finalize", {"status": "CANCELED"})
            if r.status_code in (200, 204):
                log.info(f"  [{label}] ✓ Finalized {kind} flow {flow_id} (station {flow['station']['id']})")
            else:
                log.info(f"  [{label}] ✗ Could not finalize flow {flow_id} — {r.status_code}: {r.text}")
        except Exception as e:
            log.info(f"  [{label}] Exception finalizing flow {flow_id}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — CSV PARSING & WAVE BUILDING
# ═══════════════════════════════════════════════════════════════════════════════

def load_waves_from_csv(csv_path: str, wave_size: int) -> tuple[list[Wave], dict]:
    """
    Parse order CSV and build waves of `wave_size` batches each.

    Returns:
        waves        — list of Wave objects (batch names + induction orders, CSV order)
        batch_orders — dict: batch_name -> [order_names] (used later for stuck-batch retry)
    """
    batch_order = []          # unique batch names in first-appearance (priority) order
    seen_batches = set()
    batch_orders = defaultdict(list)   # batch_name -> [order_names requiring induction]

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # Strip all keys to handle trailing spaces in header
            row = {k.strip(): v for k, v in row.items()}
            batch_name = row.get("Batch name", "").strip()
            order_id = row.get("Order ID", "").strip()
            requires_induction = row.get("Order requires induction", "").strip().upper() == "TRUE"

            if not batch_name:
                continue
            if batch_name not in seen_batches:
                seen_batches.add(batch_name)
                batch_order.append(batch_name)

            if order_id and requires_induction:
                batch_orders[batch_name].append(order_id)

    log.info(f"[bootstrap] CSV loaded: {len(batch_order)} unique batch(es), "
             f"{sum(len(v) for v in batch_orders.values())} induction order(s).")

    # Split into waves
    waves = []
    for i in range(0, len(batch_order), wave_size):
        chunk = batch_order[i:i + wave_size]
        orders = []
        for b in chunk:
            orders.extend(batch_orders[b])
        wave = Wave(
            wave_num=i // wave_size + 1,
            batch_names=chunk,
            orders=orders,
        )
        waves.append(wave)
        log.info(f"[bootstrap] Wave {wave.wave_num}: batches={chunk}  induction_orders={len(orders)}")

    return waves, dict(batch_orders)


def load_previously_inducted(log_path: str) -> set:
    """Read a previous inducted orders log and return the set of order names already done."""
    orders = set()
    if not os.path.exists(log_path):
        log.info(f"[resume] Log file not found: {log_path} — starting fresh.")
        return orders
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped and stripped[0].isdigit() and ". " in stripped:
                order_name = stripped.split(". ", 1)[1].strip()
                if order_name:
                    orders.add(order_name)
    log.info(f"[resume] Loaded {len(orders)} previously inducted order(s) from: {log_path}")
    log.info(f"[resume] These orders will be skipped in this run.")
    return orders


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — BATCH ALLOCATION
# ═══════════════════════════════════════════════════════════════════════════════

def get_mhe_id() -> str:
    items = get(
        f"{WMS_URL}/materialHandlingEquipments",
        {"type": "RR_ASRS", "page": 1, "size": 100},
    ).get("items", [])
    if not items:
        raise RuntimeError("No RR_ASRS MHE found.")
    mhe = items[0]
    log.info(f"[bootstrap] MHE: {mhe['name']}  (id={mhe['id']})")
    return mhe["id"]


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
    log.info(f"[bootstrap] Fetched {len(fragments)} allocatable batch fragment(s) from API.")
    return fragments


def resolve_batch_ids(waves: list[Wave], fragments: list[dict]) -> list[str]:
    """
    Attach fragment IDs to each wave's batch_fragment_ids list.
    Returns list of batch names that could NOT be resolved (not found in API).
    """
    fragment_map = {f["name"]: f["id"] for f in fragments}
    unresolved = []
    for wave in waves:
        ids = []
        for name in wave.batch_names:
            fid = fragment_map.get(name)
            if fid:
                ids.append(fid)
            else:
                log.info(f"  [bootstrap] ⚠ Batch '{name}' not found in allocatable fragments — skipping.")
                unresolved.append(name)
        wave.batch_fragment_ids = ids
    return unresolved


def get_batch_fragment_by_id(fragment_id: str) -> dict:
    items = get(
        f"{WMS_URL}/orderBatchFragments",
        {"id": fragment_id, "page": 1, "size": 100},
    ).get("items", [])
    if not items:
        raise RuntimeError(f"Batch fragment {fragment_id} not found.")
    return items[0]


def allocate_wave_post(wave: Wave, mhe_id: str) -> bool:
    """Single POST to allocate all batches in a wave at once."""
    body = {
        "allocateInSequence": True,
        "materialHandlingEquipment": {"id": mhe_id},
        "orderBatchFragmentIds": wave.batch_fragment_ids,
        "type": ALLOCATION_TYPE,
        "user": ALLOCATION_USER,
    }
    r = post(f"{WMS_URL}/orderBatchFragments/allocate", body)
    if r.status_code in (200, 201, 202, 204):
        log.info(f"[wave {wave.wave_num}] Allocation POST accepted (status={r.status_code}).")
        return True
    log.info(f"[wave {wave.wave_num}] Allocation POST failed — {r.status_code}: {r.text}")
    return False


def strict_poll_wave(wave: Wave, batch_orders: dict) -> tuple[list[str], list[str]]:
    """
    Poll each batch in the wave until confirmed allocated or retries exhausted.

    Returns:
        confirmed_orders — order names whose batch confirmed allocation (push to queue)
        stuck_names      — batch names that stayed ASSIGNABLE after all retries
    """
    confirmed_orders = []
    stuck_names = []

    for name, frag_id in zip(wave.batch_names, wave.batch_fragment_ids):
        allocated = False
        for attempt in range(1, ALLOC_POLL_RETRIES + 1):
            try:
                fragment = get_batch_fragment_by_id(frag_id)
                status = fragment.get("status", "UNKNOWN")

                if status in _FAILED_STATUSES:
                    log.info(f"  [wave {wave.wave_num}] ✗ '{name}' status={status} — treating as stuck.")
                    break

                if status not in _PENDING_STATUSES:
                    log.info(f"  [wave {wave.wave_num}] ✓ '{name}' confirmed (status={status}) "
                             f"on attempt {attempt}/{ALLOC_POLL_RETRIES}.")
                    confirmed_orders.extend(batch_orders.get(name, []))
                    allocated = True
                    break

                log.info(f"  [wave {wave.wave_num}] '{name}' still {status} "
                         f"(attempt {attempt}/{ALLOC_POLL_RETRIES}) — waiting {ALLOC_POLL_INTERVAL}s …")
                time.sleep(ALLOC_POLL_INTERVAL)
            except Exception as e:
                log.info(f"  [wave {wave.wave_num}] Poll error for '{name}': {e}")
                time.sleep(ALLOC_POLL_INTERVAL)

        if not allocated:
            log.info(f"  [wave {wave.wave_num}] ⚠ '{name}' stuck after "
                     f"{ALLOC_POLL_RETRIES} attempts — will retry later.")
            stuck_names.append(name)

    return confirmed_orders, stuck_names


def get_assigned_pick_count() -> int:
    data = get(
        f"{OWM_URL}/operations",
        {"kind": "PICK", "status": "ASSIGNED", "page": 1, "size": 1},
    )
    return data.get("total", 0)


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — ORDER INDUCTION  (background thread)
# ═══════════════════════════════════════════════════════════════════════════════

_induction_thread = None
_induction_stop = threading.Event()


def get_stations_by_capability(capability: str) -> list:
    return get(
        f"{OWM_URL}/stations", {"capabilities": capability, "page": 1, "size": 100}
    ).get("items", [])


def ensure_flow(station_id: int, kind: str, user: str, metadata: dict = {}) -> None:
    """Create a flow of the given kind if one isn't already ASSIGNED for that station."""
    existing = get(
        f"{OWM_URL}/flows",
        {"kind": kind, "status": "ASSIGNED", "stationIds": station_id, "page": 1, "size": 10},
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
        log.info(f"  [flow] Created {kind} flow for station {station_id}.")
    else:
        log.info(f"  [flow] Failed to create {kind} flow for station {station_id} "
                 f"— {r.status_code}: {r.text}")


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
        log.info(f"  [induction] Container {container_id} linked for order '{order_name}'.")
        return True
    log.info(f"  [induction] linkInductedContainerPartition failed — {r.status_code}: {r.text}")
    return False


def _induction_loop(
    mhe_id: str,
    stow_station_ids: list,
    induction_queue: queue.Queue,
    already_inducted: set,
) -> None:
    """
    Background induction thread.
    - Blocks on induction_queue.get() when no orders are available yet (between waves).
    - Processes READY ORDER_INDUCTION ops.
    - Stops when queue is empty AND all_orders_queued event is set.
    """
    log.info(f"[induction] Background thread started.")
    ok = skip = 0

    # Event set by main thread once all waves are queued (including retries)
    # Thread checks this to know when it's truly done vs just waiting for next wave
    all_queued = _all_orders_queued

    while not _induction_stop.is_set():
        # Ensure ORDER_INDUCTION flows alive
        try:
            ensure_induction_flows(stow_station_ids)
        except Exception as e:
            log.info(f"  [induction] Could not ensure flows: {e}")

        # Fetch all READY induction ops
        try:
            ops = get(
                f"{OWM_URL}/operations",
                {"kind": "ORDER_INDUCTION", "status": "READY", "page": 1, "size": 100},
            ).get("items", [])
        except Exception as e:
            log.info(f"  [induction] Error fetching ops: {e}")
            _induction_stop.wait(INDUCTION_POLL)
            continue

        if not ops:
            # Check if we're truly done
            if all_queued.is_set() and induction_queue.empty():
                log.info(f"[induction] All orders inducted and no READY ops — finishing.")
                stop_flows("ORDER_INDUCTION", stow_station_ids, "induction")
                break
            log.info(f"  [induction] No READY ops — waiting {INDUCTION_POLL}s …")
            _induction_stop.wait(INDUCTION_POLL)
            continue

        for op in ops:
            if _induction_stop.is_set():
                break
            op_id = op["id"]

            # Get bin + container details
            try:
                full_op = get(f"{OWM_URL}/operations/{op_id}")
                bin_id = full_op["sourceBin"]["id"]
                bin_data = get_bin(bin_id)
                container_id = bin_data.get("containerId")
            except Exception as e:
                log.info(f"  [induction] Could not fetch op/bin details for op {op_id}: {e}")
                skip += 1
                continue

            if not container_id:
                log.info(f"  [induction] No containerId for op {op_id} — skipping.")
                skip += 1
                continue

            container_barcode = bin_data.get("containerBarcode", "")
            log.info(f"  [induction] Op {op_id} → bin={bin_id}  container={container_id}  barcode={container_barcode}")

            # Get next order from queue — blocks if between waves, times out to check stop event
            order_name = None
            while not _induction_stop.is_set():
                # Skip already inducted orders (resume case)
                try:
                    candidate = induction_queue.get(timeout=INDUCTION_POLL)
                    with _inducted_lock:
                        already_done = candidate in _inducted_orders
                    if already_done:
                        log.info(f"  [induction] Skipping already inducted order '{candidate}'.")
                        induction_queue.task_done()
                        continue
                    order_name = candidate
                    break
                except queue.Empty:
                    if all_queued.is_set() and induction_queue.empty():
                        log.info(f"  [induction] No more orders in queue — all inducted!")
                        stop_flows("ORDER_INDUCTION", stow_station_ids, "induction")
                        _induction_stop.set()
                        break
                    log.info(f"  [induction] Waiting for next wave orders …")

            if order_name is None or _induction_stop.is_set():
                break

            # Set op in progress
            if not set_op_in_progress(op_id):
                log.info(f"  [induction] Could not set op {op_id} IN_PROGRESS — re-queuing '{order_name}'.")
                induction_queue.put(order_name)
                skip += 1
                continue

            log.info(f"  [induction] Inducting '{order_name}' (container={container_id})")

            # Link container to order
            try:
                if not link_inducted_container(mhe_id, container_id, order_name):
                    induction_queue.put(order_name)
                    skip += 1
                    continue
            except Exception as e:
                log.info(f"  [induction] Link exception: {e}")
                induction_queue.put(order_name)
                skip += 1
                continue

            # Log results
            try:
                r = post(
                    f"{OWM_URL}/operations/{op_id}/logResults",
                    {"result": "SUCCESS", "metadata": None, "isFinal": True},
                )
                log.info(f"  [induction] op {op_id} logResults → {r.status_code}")
                with _inducted_lock:
                    _inducted_orders.add(order_name)
                write_induction_summary()
                ok += 1
                with _inducted_lock:
                    inducted_count = len(_inducted_orders)
                log.info(f"  [induction] ✓ Inducted {inducted_count}/{_total_orders} orders so far.")
            except Exception as e:
                log.info(f"  [induction] logResults exception (non-fatal): {e}")
                with _inducted_lock:
                    _inducted_orders.add(order_name)
                ok += 1

            induction_queue.task_done()
            time.sleep(5)  # brief pause between inductions

    log.info(f"[induction] Thread stopped — ✓ {ok} inducted  ✗ {skip} skipped.")
    with _inducted_lock:
        log.info(f"[induction] Total unique orders inducted: {len(_inducted_orders)}")


# Event set by main thread when all waves (including retried ones) have been queued
_all_orders_queued = threading.Event()


def start_induction_thread(mhe_id: str, stow_station_ids: list, induction_queue: queue.Queue) -> None:
    global _induction_thread, _induction_stop
    if _induction_thread and _induction_thread.is_alive():
        return
    _induction_stop.clear()
    _induction_thread = threading.Thread(
        target=_induction_loop,
        args=(mhe_id, stow_station_ids, induction_queue, set()),
        daemon=True,
        name="induction",
    )
    _induction_thread.start()


def stop_induction_thread() -> None:
    _induction_stop.set()
    if _induction_thread:
        _induction_thread.join(timeout=15)


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — BATCHLESS PICKING  (background thread)
# ═══════════════════════════════════════════════════════════════════════════════

_pick_thread = None
_pick_stop = threading.Event()


def _pick_loop() -> None:
    log.info("[picking] Background thread started — ensuring batchless PICK flows.")
    while not _pick_stop.is_set():
        try:
            stations = get_stations_by_capability("PICK")
            for s in stations:
                ensure_flow(
                    s["id"],
                    "PICK",
                    ALLOCATION_USER,
                    metadata={"externalOrderBatchFragmentId": None},
                )
        except Exception as e:
            log.info(f"  [picking] Error: {e}")
        _pick_stop.wait(PICK_POLL)
    log.info("[picking] Background thread stopped.")


def start_pick_thread() -> None:
    global _pick_thread, _pick_stop
    if _pick_thread and _pick_thread.is_alive():
        return
    _pick_stop.clear()
    _pick_thread = threading.Thread(target=_pick_loop, daemon=True, name="picking")
    _pick_thread.start()


def stop_pick_thread() -> None:
    _pick_stop.set()
    if _pick_thread:
        _pick_thread.join(timeout=15)


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — DROP-OFF  (background thread)
# ═══════════════════════════════════════════════════════════════════════════════

_drop_thread = None
_drop_stop = threading.Event()


def _ensure_drop_flows(station_ids: list) -> None:
    try:
        active = get(
            f"{OWM_URL}/flows",
            params=[("kind", "DROP"), ("status", "ASSIGNED"), ("page", 1), ("size", 100)]
            + [("stationIds", sid) for sid in station_ids],
        ).get("items", [])
        active_station_ids = {f["station"]["id"] for f in active}
    except Exception as e:
        log.info(f"  [dropoff] Could not fetch active DROP flows: {e}")
        return
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
                log.info(f"  [dropoff] Created DROP flow for station {sid}.")
            elif r.status_code == 409:
                pass  # already exists — race condition, safe to ignore
            else:
                log.info(f"  [dropoff] Could not create DROP flow for station {sid} "
                         f"— {r.status_code}: {r.text}")


def _drop_loop(station_ids: list) -> None:
    log.info(f"[dropoff] Background thread started for stations {station_ids}.")
    while not _drop_stop.is_set():
        try:
            _ensure_drop_flows(station_ids)

            ops = get(
                f"{OWM_URL}/operations",
                params=[("kind", "DROP"), ("status", "READY"), ("page", 1), ("size", 100)]
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
                    log.info(f"  [dropoff] Could not fetch bin {source_bin_id}: {e}")
                    continue

                if not container_id:
                    log.info(f"  [dropoff] No containerId for bin {source_bin_id} — skipping.")
                    continue

                bin_role = bin_data.get("role")
                if bin_role != "ORDER":
                    log.info(f"  [dropoff] SKU bin {barcode} (role={bin_role}) — canceling op {op_id}")
                    r = post(f"{OWM_URL}/operations/{op_id}/finalize", {"status": "CANCELED"})
                    if r.status_code in (200, 204):
                        log.info(f"  [dropoff] ✓ Op {op_id} canceled.")
                    else:
                        log.info(f"  [dropoff] ✗ Failed to cancel op {op_id} — {r.status_code}: {r.text}")
                    continue

                set_op_in_progress(op_id)
                r = post(
                    f"{OWM_URL}/operations/{op_id}/logResults",
                    {"result": "SUCCESS", "metadata": {"type": "CONTENT_SHIPPING"}},
                )
                log.info(f"  [dropoff] Drop-off for {barcode} (op {op_id}) … status={r.status_code}")

        except Exception as e:
            log.info(f"  [dropoff] Error: {e}")

        _drop_stop.wait(DROP_POLL)

    log.info("[dropoff] Background thread stopped.")


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
    parser = argparse.ArgumentParser(description="Coordination Script v3 — tc_003 end-to-end")
    parser.add_argument("--orders-csv", required=True,
                        help="Path to the orders CSV file")
    parser.add_argument("--log-dir", default=".",
                        help="Directory to write log files (default: current directory)")
    parser.add_argument("--wave-size", type=int, default=5,
                        help="Number of batches to allocate per wave (default: 5)")
    parser.add_argument("--resume-log", default=None,
                        help="Path to a previous inducted_orders.log to resume from")
    args = parser.parse_args()

    global _total_orders

    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    setup_logging(args.log_dir, timestamp)

    log.info("=" * 65)
    log.info("Coordination Script v3 — tc_003 end-to-end")
    log.info("Pre-conditions: upload ✓  magical-stow ✓  reserve ✓")
    log.info("=" * 65)

    # ── Bootstrap ─────────────────────────────────────────────────────────────

    mhe_id = get_mhe_id()

    # Parse CSV → build waves
    waves, batch_orders = load_waves_from_csv(args.orders_csv, args.wave_size)
    _total_orders = sum(len(v) for v in batch_orders.values())
    log.info(f"[bootstrap] {len(waves)} wave(s) of up to {args.wave_size} batch(es) each. "
             f"Total induction orders: {_total_orders}")

    if not waves:
        log.info("[bootstrap] ✗ No batches found in CSV — check column names. Exiting.")
        teardown_logging()
        sys.exit(1)

    # Resume — pre-load previously inducted orders
    if args.resume_log:
        previously_inducted = load_previously_inducted(args.resume_log)
        with _inducted_lock:
            _inducted_orders.update(previously_inducted)
        log.info(f"[resume] {len(previously_inducted)} order(s) pre-loaded — will not be re-inducted.")
    else:
        log.info("[resume] No resume log provided — starting fresh.")

    # Resolve batch names → fragment IDs (once, upfront)
    all_fragments = fetch_all_batch_fragments()
    unresolved = resolve_batch_ids(waves, all_fragments)
    if unresolved:
        log.info(f"[bootstrap] ⚠ {len(unresolved)} batch name(s) could not be resolved: {unresolved}")

    # Discover stations
    pick_stations = [s["id"] for s in get_stations_by_capability("PICK")]
    drop_stations = DROP_STATION_IDS_OVERRIDE or [
        s["id"] for s in get_stations_by_capability("DROP")
    ]
    stow_stations = STOW_STATION_IDS_OVERRIDE or [
        s["id"] for s in get_stations_by_capability("STOW")
    ]
    log.info(f"[bootstrap] PICK stations : {pick_stations}")
    log.info(f"[bootstrap] DROP stations : {drop_stations} "
             f"{'(manual override)' if DROP_STATION_IDS_OVERRIDE else '(auto-discovered)'}")
    log.info(f"[bootstrap] STOW stations : {stow_stations} "
             f"{'(manual override)' if STOW_STATION_IDS_OVERRIDE else '(auto-discovered)'}")

    # Induction queue — fed wave by wave from main thread
    induction_queue = queue.Queue()

    # Ensure induction + drop flows are alive before starting threads
    # (covers resume case where flows may have gone missing)
    ensure_induction_flows(stow_stations)
    _ensure_drop_flows(drop_stations)

    # Start all 3 background threads
    start_induction_thread(mhe_id, stow_stations, induction_queue)
    start_pick_thread()
    start_dropoff_thread(drop_stations)

    # Track stuck batches for end-of-run retry
    failed_waves: list[Wave] = []

    try:
        # ── Phase 1: Allocate all waves ────────────────────────────────────────
        for wave in waves:
            # Wait until assigned pick ops are below threshold
            while True:
                ops = get_assigned_pick_count()
                log.info(f"\n[coordinator] Assigned pick ops: {ops}  |  "
                         f"Wave {wave.wave_num}/{len(waves)}")
                if ops <= ASSIGNED_OP_THRESHOLD:
                    break
                log.info(f"[coordinator] Ops > {ASSIGNED_OP_THRESHOLD} — waiting {POLL_INTERVAL}s …")
                time.sleep(POLL_INTERVAL)

            log.info(f"\n{'─' * 65}")
            log.info(f"[wave {wave.wave_num}] Allocating {len(wave.batch_fragment_ids)} batch(es): "
                     f"{wave.batch_names}")

            if not wave.batch_fragment_ids:
                log.info(f"[wave {wave.wave_num}] No resolvable batch IDs — skipping.")
                continue

            allocated_ok = allocate_wave_post(wave, mhe_id)
            if not allocated_ok:
                log.info(f"[wave {wave.wave_num}] ⚠ POST failed — all batches in wave treated as stuck.")
                failed_waves.append(wave)
                continue

            # Strict poll — confirm each batch allocated
            confirmed_orders, stuck_names = strict_poll_wave(wave, batch_orders)

            # Push confirmed orders to induction queue
            if confirmed_orders:
                skipped_resume = 0
                with _inducted_lock:
                    already_done = set(_inducted_orders)
                for order in confirmed_orders:
                    if order not in already_done:
                        induction_queue.put(order)
                    else:
                        skipped_resume += 1
                log.info(f"[wave {wave.wave_num}] ✓ {len(confirmed_orders)} order(s) pushed to induction queue "
                         f"({skipped_resume} skipped — already inducted).")

            # Track stuck batches
            if stuck_names:
                stuck_wave = Wave(
                    wave_num=wave.wave_num,
                    batch_names=stuck_names,
                    batch_fragment_ids=[
                        fid for name, fid in zip(wave.batch_names, wave.batch_fragment_ids)
                        if name in stuck_names
                    ],
                    orders=[o for name in stuck_names for o in batch_orders.get(name, [])],
                )
                failed_waves.append(stuck_wave)
                log.info(f"[wave {wave.wave_num}] ⚠ {len(stuck_names)} stuck batch(es) queued for retry: {stuck_names}")

        # ── Phase 2: Retry stuck batches ──────────────────────────────────────
        if failed_waves:
            log.info(f"\n{'─' * 65}")
            log.info(f"[coordinator] Retrying {len(failed_waves)} stuck wave(s) …")
            for wave in failed_waves:
                ops = get_assigned_pick_count()
                log.info(f"[coordinator] Assigned pick ops: {ops}  |  Retrying wave {wave.wave_num}")
                if ops > ASSIGNED_OP_THRESHOLD:
                    log.info(f"[coordinator] Ops > {ASSIGNED_OP_THRESHOLD} — waiting {POLL_INTERVAL}s …")
                    time.sleep(POLL_INTERVAL)

                log.info(f"[retry] Wave {wave.wave_num}: batches={wave.batch_names}")
                allocated_ok = allocate_wave_post(wave, mhe_id)
                if not allocated_ok:
                    log.info(f"[retry] ⚠ POST failed again for wave {wave.wave_num} — permanently skipping.")
                    continue

                confirmed_orders, still_stuck = strict_poll_wave(wave, batch_orders)
                if confirmed_orders:
                    with _inducted_lock:
                        already_done = set(_inducted_orders)
                    for order in confirmed_orders:
                        if order not in already_done:
                            induction_queue.put(order)
                    log.info(f"[retry] Wave {wave.wave_num}: {len(confirmed_orders)} order(s) pushed to induction queue.")

                if still_stuck:
                    log.info(f"[retry] ⚠ Permanently stuck batches (gave up): {still_stuck}")

        # Signal induction thread: all waves have been queued
        _all_orders_queued.set()
        log.info(f"\n[coordinator] All waves processed. Induction queue fully populated.")

        # ── Phase 3: Wait for pick ops to drain ───────────────────────────────
        log.info(f"\n[coordinator] Waiting for pick ops to drain …")
        while True:
            ops = get_assigned_pick_count()
            if ops == 0:
                log.info("[coordinator] ✓ All pick ops completed.")
                break
            log.info(f"[coordinator] {ops} pick op(s) remaining — waiting {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)

        # ── Phase 4: Wait for drop-off ops to drain ───────────────────────────
        log.info(f"\n[coordinator] Waiting for drop-off ops to drain …")
        while True:
            drop_ops = get(
                f"{OWM_URL}/operations",
                {"kind": "DROP", "status": "READY", "page": 1, "size": 1},
            ).get("total", 0)
            if drop_ops == 0:
                log.info("[coordinator] ✓ All drop-off ops completed.")
                break
            log.info(f"[coordinator] {drop_ops} drop-off op(s) remaining — waiting {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)

        # ── Phase 5: Idle — keep drop-off running until Ctrl+C ────────────────
        log.info("\n[coordinator] All ops done. Drop-off thread still running.")
        log.info("[coordinator] Press Ctrl+C to stop and exit.")
        while True:
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        log.info("\n[coordinator] Interrupted by user.")
        log.info("[coordinator] Flows left alive in stations — safe to resume.")
        # NOTE: ORDER_INDUCTION and DROP flows are intentionally NOT cancelled here.
        # Leaving them alive means a resume run can reconnect without recreating flows.

    finally:
        log.info("[coordinator] Stopping background threads …")
        _all_orders_queued.set()   # unblock induction thread if waiting
        stop_induction_thread()
        stop_pick_thread()
        stop_dropoff_thread()
        log.info("[coordinator] Exited cleanly.")
        write_induction_summary()
        teardown_logging()


if __name__ == "__main__":
    main()
