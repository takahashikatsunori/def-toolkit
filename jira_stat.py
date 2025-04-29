import json
import csv
import argparse
from datetime import datetime, date, time, timedelta, timezone
from collections import defaultdict
import os


def parse_iso(dt_str):
    # ISO 8601 string to timezone-aware datetime
    try:
        dt = datetime.fromisoformat(dt_str)
    except ValueError:
        # Fallback for strings without offset
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def extract_field_counts(json_path, field_id, debug=False):
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])

    ticket_events = {}
    all_statuses = set()

    for issue in issues:
        key = issue.get('key')
        created_dt = parse_iso(issue['fields']['created'])
        events = []
        # initial status determination
        histories = issue.get('changelog', {}).get('histories', [])
        history_events = []
        for hist in histories:
            ts = parse_iso(hist.get('created'))
            for item in hist.get('items', []):
                if item.get('field') == field_id:
                    from_s = item.get('fromString') or None
                    to_s = item.get('toString') or None
                    history_events.append((ts, from_s, to_s))
        history_events.sort(key=lambda x: x[0])
        # determine initial status
        if history_events and history_events[0][1]:
            init_status = history_events[0][1]
        else:
            # fallback to current field value
            val = issue['fields'].get(field_id)
            if isinstance(val, dict):
                init_status = val.get('name')
            else:
                init_status = val
        # record creation as midnight UTC event
        events.append((datetime.combine(created_dt.date(), time(0), tzinfo=timezone.utc), init_status))
        all_statuses.add(init_status)
        # record transitions
        for ts, _from, to in history_events:
            events.append((ts, to))
            all_statuses.add(to)
            if debug:
                print(f"[{key}] Event: {ts.isoformat()} -> {to}")
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events

    # determine date range
    dates = []
    for events in ticket_events.values():
        for ts, _ in events:
            dates.append(ts.date())
    if not dates:
        return [], []
    start_date = min(dates)
    end_date = datetime.now(timezone.utc).date()

    # prepare daily counts
    status_list = sorted(all_statuses)
    daily_counts = []
    for single_date in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1)):
        snapshot = datetime.combine(single_date, time(0), tzinfo=timezone.utc)
        count = defaultdict(int)
        for key, events in ticket_events.items():
            current = None
            for ts, status in events:
                if ts <= snapshot:
                    current = status
                    if debug:
                        print(f"[{key}] matched {status} at {ts.isoformat()} for {single_date}")
                else:
                    break
            if current:
                count[current] += 1
        row = [single_date.isoformat()] + [count[s] for s in status_list]
        daily_counts.append(row)

    # write CSV(s)
    stat_default = 'stat_status.csv'
    with open(stat_default, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date'] + status_list)
        writer.writerows(daily_counts)
    extra_file = None
    if field_id != 'status':
        extra_file = f'stat_{field_id}.csv'
        with open(extra_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Date'] + status_list)
            writer.writerows(daily_counts)

    return [stat_default] + ([extra_file] if extra_file else []), status_list


def extract_flow_counts(json_path, config_path, field_id, debug=False):
    # load issues and history
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])

    # gather all history transitions
    transitions = []  # list of (date, from, to)
    statuses = set()
    for issue in issues:
        histories = issue.get('changelog', {}).get('histories', [])
        for hist in histories:
            ts = parse_iso(hist.get('created'))
            for item in hist.get('items', []):
                if item.get('field') == field_id:
                    _from = item.get('fromString') or ''
                    _to = item.get('toString') or ''
                    datestr = ts.date().isoformat()
                    transitions.append((datestr, _from, _to))
                    statuses.update([_from, _to])
    # config generation
    if not os.path.exists(config_path):
        matrix = {s: {t: 'IGNORE' for t in statuses} for s in statuses}
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(matrix, f, ensure_ascii=False, indent=2)
        print(f"{config_path} が生成されました。編集後、再度実行してください。")
        return None

    # load config
    with open(config_path, encoding='utf-8') as f:
        config = json.load(f)

    flow_counts = defaultdict(lambda: {'IN': 0, 'OUT': 0})
    for datestr, _from, _to in transitions:
        action = config.get(_from, {}).get(_to, 'IGNORE')
        if action in ('IN', 'OUT'):
            flow_counts[datestr][action] += 1
            if debug:
                print(f"Transition on {datestr}: {_from} -> {_to} as {action}")

    # write in-out_flow.csv
    out_file = 'in-out_flow.csv'
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'IN', 'OUT'])
        for dt in sorted(flow_counts):
            writer.writerow([dt, flow_counts[dt]['IN'], flow_counts[dt]['OUT']])

    return out_file


def main():
    parser = argparse.ArgumentParser(
        description='Generate daily status counts and IN/OUT flows from JIRA JSON')
    parser.add_argument('input_json', nargs='?', default='output.json', help='JIRA JSON file path')
    parser.add_argument('field_id', nargs='?', default='status', help='Field ID to analyze')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    csv_files, statuses = extract_field_counts(args.input_json, args.field_id, args.debug)
    if not csv_files:
        print("No data to process.")
        return

    flow = extract_flow_counts(args.input_json, 'in-out_config.json', args.field_id, args.debug)

    outputs = csv_files + ([flow] if flow else [])
    print(f"統計結果を {', '.join(outputs)} に出力しました。")


if __name__ == '__main__':
    main()
