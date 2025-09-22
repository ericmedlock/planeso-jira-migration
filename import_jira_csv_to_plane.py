
#!/usr/bin/env python3
"""
Import Jira CSV into a Plane (self-hosted) Community Edition project using the public REST API.

Requirements:
    pip install requests python-dotenv

Usage:
    1) Copy plane_import.env.example to .env and fill in values
    2) Export a CSV from Jira (all fields recommended). Save it as jira_export.csv
    3) Run:
        python import_jira_csv_to_plane.py --csv jira_export.csv --dry-run
        python import_jira_csv_to_plane.py --csv jira_export.csv
Notes:
    - The script maps basic fields: Summary -> name, Description -> description.
    - Optional: Labels, Status, Priority. Unknown or missing fields are skipped safely.
    - Labels and States are created in Plane if missing (opt-out with flags).

Tested against Plane API docs (Add issue, List/Create labels, List/Create states).
"""
import argparse
import csv
import os
import sys
import time
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

# --------------------------
# Config & helpers
# --------------------------

def env(name: str, required: bool = True) -> str:
    val = os.getenv(name, "").strip()
    if required and not val:
        print(f"[FATAL] Missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return val

def plane_headers(api_key: str) -> Dict[str, str]:
    return {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def get(url: str, headers: Dict[str, str]):
    r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
    ctype = r.headers.get('Content-Type', '')
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text[:200]}")
    try:
        if 'application/json' not in ctype:
            raise ValueError(f"Non-JSON response (Content-Type={ctype}). First bytes: {r.text[:120]}")
        return r.json()
    except Exception as e:
        raise RuntimeError(f"GET {url} -> {r.status_code}; JSON parse error: {e}; First bytes: {r.text[:120]}")

def post(url: str, headers: Dict[str, str], json: dict):
    r = requests.post(url, headers=headers, json=json, timeout=30)
    ctype = r.headers.get('Content-Type', '')
    if r.status_code >= 400:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text[:200]}")
    try:
        if 'application/json' not in ctype:
            raise ValueError(f"Non-JSON response (Content-Type={ctype}). First bytes: {r.text[:120]}")
        return r.json()
    except Exception as e:
        raise RuntimeError(f"POST {url} -> {r.status_code}; JSON parse error: {e}; First bytes: {r.text[:120]}")

# --------------------------
# Plane lookups
# --------------------------

def list_labels(base_url: str, ws_slug: str, project_id: str, headers: Dict[str,str]) -> Dict[str, dict]:
    url = f"{base_url}/api/v1/workspaces/{ws_slug}/projects/{project_id}/labels/"
    data = get(url, headers)
    # normalize by case-insensitive name
    return { (item.get("name") or "").strip().lower(): item for item in data.get("results", data) }

def ensure_label(base_url: str, ws_slug: str, project_id: str, headers: Dict[str,str], name: str) -> dict:
    existing = list_labels(base_url, ws_slug, project_id, headers)
    key = (name or "").strip().lower()
    if key in existing:
        return existing[key]
    # create
    url = f"{base_url}/api/v1/workspaces/{ws_slug}/projects/{project_id}/labels/"
    created = post(url, headers, {"name": name})
    return created


def infer_state_group(state_name: str) -> str:
    name = (state_name or '').strip().lower()
    if name in ('backlog','to do','todo','unstarted','selected for development'):
        return 'backlog'
    if name in ('in progress','started','in review','doing'):
        return 'started'
    if name in ('done','completed','closed','resolved'):
        return 'completed'
    if name in ('cancelled','canceled','wontfix','won\'t fix'):
        return 'cancelled'
    return 'backlog'


def list_states(base_url: str, ws_slug: str, project_id: str, headers: Dict[str,str]) -> Dict[str, dict]:
    url = f"{base_url}/api/v1/workspaces/{ws_slug}/projects/{project_id}/states/"
    data = get(url, headers)
    return { (item.get("name") or "").strip().lower(): item for item in data.get("results", data) }

def ensure_state(base_url: str, ws_slug: str, project_id: str, headers: Dict[str,str], name: str) -> dict:
    existing = list_states(base_url, ws_slug, project_id, headers)
    key = (name or "").strip().lower()
    if key in existing:
        return existing[key]
    url = f"{base_url}/api/v1/workspaces/{ws_slug}/projects/{project_id}/states/"
    # Try minimal payload with default neutral color
    payload = {"name": name, "color": "#9ca3af"}
    try:
        return post(url, headers, payload)
    except Exception as e:
        # If server requires a group, retry with inferred group
        grp = infer_state_group(name)
        payload["group"] = grp
        try:
            return post(url, headers, payload)
        except Exception as e2:
            raise

def create_issue(base_url: str, ws_slug: str, project_id: str, headers: Dict[str,str], payload: dict) -> dict:
    url = f"{base_url}/api/v1/workspaces/{ws_slug}/projects/{project_id}/issues/"
    return post(url, headers, payload)

# --------------------------
# CSV parsing and mapping
# --------------------------

DEFAULT_LABEL_SEP = ";"

# Conservative priority mapping; adjust per your Plane setup.
DEFAULT_PRIORITY_MAP = {
    "Highest": "urgent",
    "High": "high",
    "Medium": "medium",
    "Low": "low",
    "Lowest": "low",
}

# Map Jira Status -> Plane State
DEFAULT_STATUS_TO_STATE = {
    "To Do": "Backlog",
    "Selected for Development": "Unstarted",
    "In Progress": "Started",
    "In Review": "Started",
    "Done": "Completed",
    "Closed": "Completed",
}

def map_priority(jira_priority: str) -> Optional[str]:
    if not jira_priority:
        return None
    return DEFAULT_PRIORITY_MAP.get(jira_priority.strip(), None)

def map_state(jira_status: str) -> Optional[str]:
    if not jira_status:
        return None
    return DEFAULT_STATUS_TO_STATE.get(jira_status.strip(), None)

def parse_labels(raw: str, sep: str = DEFAULT_LABEL_SEP) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace(",", sep).split(sep)]
    return [p for p in parts if p]

# --------------------------
# Main
# --------------------------


def resolve_project_id(base_url: str, ws_slug: str, headers: Dict[str,str], want: str) -> str:
    """Resolve a project by id, identifier, slug, or name via List Projects. Return id."""
    url = f"{base_url}/api/v1/workspaces/{ws_slug}/projects/"
    data = get(url, headers)
    results = data.get("results", data)
    want_norm = (want or "").strip().lower()
    # If want already looks like a UUID, just return it
    import re as _re
    if _re.match(r"^[0-9a-f-]{8,}$", want_norm):
        return want
    # Match by identifier, name, or slug (case-insensitive)
    for proj in results:
        pid = proj.get("id") or proj.get("project_id")
        ident = (proj.get("identifier") or "").strip().lower()
        name = (proj.get("name") or "").strip().lower()
        slug = (proj.get("slug") or "").strip().lower()
        if want_norm in (ident, name, slug):
            if pid:
                return pid
    raise RuntimeError(f"Could not resolve project '{want}' in workspace '{ws_slug}'.")


def main():
    parser = argparse.ArgumentParser(description="Import Jira CSV into Plane")
    parser.add_argument("--csv", required=True, help="Path to Jira CSV export")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default ,)")
    parser.add_argument("--label-sep", default=DEFAULT_LABEL_SEP, help="Label separator inside CSV cells (default ;)")
    parser.add_argument("--dry-run", action="store_true", help="Do not call Plane API, just print what would happen")
    parser.add_argument("--offline", action="store_true", help="Do not call Plane at all (skip label/state fetch and creation)")
    parser.add_argument("--no-create-labels", action="store_true", help="Do not auto-create missing labels")
    parser.add_argument("--no-create-states", action="store_true", help="Do not auto-create missing states")
    parser.add_argument("--rate-limit", type=float, default=0.0, help="Seconds to sleep between issue creates to be gentle (e.g. 0.5)")
    args = parser.parse_args()

    load_dotenv()  # read .env

    base_url = env("PLANE_BASE_URL")  # e.g. https://plane.yourdomain.com or https://api.plane.so
    ws_slug = env("PLANE_WORKSPACE_SLUG")  # workspace slug
    project_id = env("PLANE_PROJECT_ID")   # May be id, identifier, slug, or name; we will resolve
    # Attempt to resolve friendly names to an id via List Projects
    try:
        project_id = resolve_project_id(base_url, ws_slug, headers, project_id)
    except Exception as e:
        print(f"[WARN] Project resolution failed ({e}). Will try using it as-is.")

    api_key = env("PLANE_API_KEY")         # X-API-Key token

    headers = plane_headers(api_key)

    # cache lookups
    label_cache = list_labels(base_url, ws_slug, project_id, headers)
    state_cache = list_states(base_url, ws_slug, project_id, headers)

    created_count = 0
    skipped = 0
    failures = 0

    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=args.delimiter)
        required_cols = ["Summary"]
        for col in required_cols:
            if col not in reader.fieldnames:
                print(f"[FATAL] CSV missing required column: {col}. Present: {reader.fieldnames}", file=sys.stderr)
                sys.exit(2)

        for i, row in enumerate(reader, start=1):
            name = (row.get("Summary") or "").strip()
            if not name:
                print(f"[SKIP] Row {i}: empty Summary")
                skipped += 1
                continue

            description = row.get("Description") or ""
            jira_labels_raw = row.get("Labels") or row.get("labels") or ""
            jira_status = row.get("Status") or row.get("status") or ""
            jira_priority = row.get("Priority") or row.get("priority") or ""

            labels = parse_labels(jira_labels_raw, sep=args.label_sep)
            target_state_name = map_state(jira_status)
            priority = map_priority(jira_priority)

            payload = {"name": name}
            if description:
                payload["description"] = description
            if priority:
                payload["priority"] = priority  # Plane accepts strings like "high" per UI; adjust if your instance differs.
            # Resolve/ensure state id if we have a target state
            if target_state_name:
                state_key = target_state_name.strip().lower()
                state_obj = state_cache.get(state_key)
                if not state_obj and not args.no_create_states:
                    if args.dry_run or args.offline:
                        print(f"[DRY] Would create state: {target_state_name}")
                        state_obj = {"name": target_state_name, "id": None}
                    else:
                        if args.offline:
                            print(f"[OFFLINE] Skipping create state: {target_state_name}")
                            state_obj = {"name": target_state_name, "id": None}
                        else:
                            state_obj = ensure_state(base_url, ws_slug, project_id, headers, target_state_name)
                            state_cache = list_states(base_url, ws_slug, project_id, headers)
                if state_obj and state_obj.get("id"):
                    payload["state_id"] = state_obj["id"]

            # Resolve/ensure labels
            label_ids: List[str] = []
            for lbl in labels:
                key = lbl.strip().lower()
                lbl_obj = label_cache.get(key)
                if not lbl_obj and not args.no_create_labels:
                    if args.dry_run or args.offline:
                        print(f"[DRY] Would create label: {lbl}")
                        lbl_obj = {"name": lbl, "id": None}
                    else:
                        if args.offline:
                            print(f"[OFFLINE] Skipping create label: {lbl}")
                            lbl_obj = {"name": lbl, "id": None}
                        else:
                            lbl_obj = ensure_label(base_url, ws_slug, project_id, headers, lbl)
                            label_cache = list_labels(base_url, ws_slug, project_id, headers)
                if lbl_obj and lbl_obj.get("id"):
                    label_ids.append(lbl_obj["id"])
            if label_ids:
                payload["label_ids"] = label_ids

            if args.dry_run or args.offline:
                print(f"[DRY] Row {i}: would create issue payload={payload}")
                created_count += 1
                continue

            try:
                resp = create_issue(base_url, ws_slug, project_id, headers, payload)
                created_count += 1
                print(f"[OK] Row {i}: created issue -> {resp.get('id') or resp}")
                if args.rate_limit > 0:
                    time.sleep(args.rate_limit)
            except Exception as e:
                failures += 1
                print(f"[FAIL] Row {i}: {e}", file=sys.stderr)

    print(f"\nDone. Created: {created_count}, Skipped: {skipped}, Failures: {failures}")


if __name__ == "__main__":
    main()
