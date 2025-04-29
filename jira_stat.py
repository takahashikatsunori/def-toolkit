import json
import csv
import argparse
from datetime import datetime, date, time, timedelta, timezone
from collections import defaultdict
import os


def parse_iso(dt_str):
    # ISO 8601 文字列をタイムゾーン対応 datetime に変換
    try:
        dt = datetime.fromisoformat(dt_str)
    except ValueError:
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

        # 初期ステータス
        if history_events and history_events[0][1]:
            init_status = history_events[0][1]
        else:
            val = issue['fields'].get(field_id)
            init_status = val.get('name') if isinstance(val, dict) else val

        events = [(datetime.combine(created_dt.date(), time(0), tzinfo=timezone.utc), init_status)]
        all_statuses.add(init_status)
        for ts, _from, to in history_events:
            events.append((ts, to))
            all_statuses.add(to)
            if debug:
                print(f"[{key}] Event: {ts.isoformat()} -> {to}")
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events

    dates = [ts.date() for evs in ticket_events.values() for ts, _ in evs]
    if not dates:
        return [], []
    start_date = min(dates)
    end_date = datetime.now(timezone.utc).date()

    status_list = sorted(all_statuses)
    daily_counts = []
    for single_date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)):
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

    stat_csv = 'stat_status.csv'
    with open(stat_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date'] + status_list)
        writer.writerows(daily_counts)

    files = [stat_csv]
    if field_id != 'status':
        extra_csv = f'stat_{field_id}.csv'
        with open(extra_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Date'] + status_list)
            writer.writerows(daily_counts)
        files.append(extra_csv)

    return files, status_list


def extract_flow_counts(json_path, config_path, field_id, debug=False):
    # JSON から履歴遷移を抽出
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])

    transitions = []  # (ticket_key, datestr, from, to)
    statuses = set()
    for issue in issues:
        key = issue.get('key')
        for hist in issue.get('changelog', {}).get('histories', []):
            ts = parse_iso(hist.get('created'))
            datestr = ts.date().isoformat()
            for item in hist.get('items', []):
                if item.get('field') == field_id:
                    _from = item.get('fromString') or ''
                    _to = item.get('toString') or ''
                    transitions.append((key, datestr, _from, _to))
                    statuses.update([_from, _to])

    # 設定ファイル初回生成
    if not os.path.exists(config_path):
        matrix = {s: {t: 'IGNORE' for t in statuses} for s in statuses}
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(matrix, f, ensure_ascii=False, indent=2)
        print(f"{config_path} を生成しました。編集後に再実行してください。")
        return None

    with open(config_path, encoding='utf-8') as f:
        config = json.load(f)

    # チケット・日付ごとに遷移をカウント
    ticket_date_counts = defaultdict(lambda: {'IN': 0, 'OUT': 0})
    for key, datestr, _from, _to in transitions:
        action = config.get(_from, {}).get(_to, 'IGNORE')
        if action == 'IN':
            ticket_date_counts[(key, datestr)]['IN'] += 1
        elif action == 'OUT':
            ticket_date_counts[(key, datestr)]['OUT'] += 1
        elif action == 'INOUT':
            ticket_date_counts[(key, datestr)]['IN'] += 1
            ticket_date_counts[(key, datestr)]['OUT'] += 1
        if debug:
            print(f"[{key}][{datestr}] {_from}->{_to} as {action}")

    # 日付範囲を決定（遷移の最古日～今日）
    if transitions:
        dates = [date.fromisoformat(d) for (_, d, _, _) in transitions]
        start_date = min(dates)
    else:
        start_date = datetime.now(timezone.utc).date()
    end_date = datetime.now(timezone.utc).date()

    # 日次フロー集計
    flow_counts = defaultdict(lambda: {'IN': 0, 'OUT': 0})
    for (key, datestr), cnts in ticket_date_counts.items():
        in_ct = cnts['IN']
        out_ct = cnts['OUT']
        # ルール適用
        if in_ct > out_ct:
            flow_counts[datestr]['IN'] += 1
        elif in_ct < out_ct:
            flow_counts[datestr]['OUT'] += 1
        else:
            flow_counts[datestr]['IN'] += 1
            flow_counts[datestr]['OUT'] += 1
        if debug:
            print(f"[{key}][{datestr}] in:{in_ct},out:{out_ct} => IN={1 if in_ct>=out_ct else 0}, OUT={1 if out_ct>=in_ct else 0}")

    # CSV 出力: すべての日付に1行ずつ出力
    out_file = 'in-out_flow.csv'
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'IN', 'OUT'])
        days = (end_date - start_date).days + 1
        for n in range(days):
            d = start_date + timedelta(days=n)
            ds = d.isoformat()
            cnts = flow_counts.get(ds, {'IN': 0, 'OUT': 0})
            writer.writerow([ds, cnts['IN'], cnts['OUT']])

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
