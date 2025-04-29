import json
import csv
import argparse
from datetime import datetime, date, time, timedelta, timezone
from collections import defaultdict

def parse_iso(dt_str):
    """ISO文字列をdatetimeに変換し、UTCタイムゾーンを設定する"""
    if not dt_str:
        return None
    try:
        if dt_str.endswith('Z'):
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return datetime.fromisoformat(dt_str)
    except ValueError:
        base = dt_str.split('.')[0]
        return datetime.fromisoformat(base)

def extract_field_counts(json_path, field_id, debug=False):
    # 出力先ファイル名を固定
    csv_path = f"stat_{field_id}.csv"

    # JSON読み込み
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])
    ticket_events = {}

    for issue in issues:
        key = issue.get('key')
        fields = issue.get('fields', {})
        # チケット作成日時
        created_dt = parse_iso(fields.get('created', ''))

        # changelogはissue直下
        histories = issue.get('changelog', {}).get('histories', [])
        history_events = []
        for hist in histories:
            hist_dt = parse_iso(hist.get('created', ''))
            if not hist_dt:
                continue
            hist_dt = hist_dt.astimezone(timezone.utc)
            for item in hist.get('items', []):
                if item.get('field') == field_id:
                    # fromString, toStringを両方取得
                    history_events.append((hist_dt, item.get('fromString'), item.get('toString')))
        # 時系列ソート
        history_events.sort(key=lambda x: x[0])

        # 初期ステータス判定
        if history_events and history_events[0][1] is not None:
            initial_status = history_events[0][1]
        else:
            # fallback: 現状のフィールド値
            if field_id == 'status':
                initial_status = fields.get('status', {}).get('name')
            else:
                raw = fields.get(field_id)
                if isinstance(raw, dict):
                    initial_status = raw.get('value') or raw.get('name')
                else:
                    initial_status = raw

        events = []
        # 初期イベント: 作成日の0時
        if created_dt and initial_status is not None:
            created_midnight = datetime.combine(created_dt.astimezone(timezone.utc).date(), time(0,0), tzinfo=timezone.utc)
            events.append((created_midnight, initial_status))
        # 履歴イベント: toStringを適用
        for dt, from_val, to_val in history_events:
            events.append((dt, to_val))
        # ソート
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events

        if debug:
            print(f"[DEBUG] {key} events:")
            for dt, val in events:
                print(f"    {dt.isoformat()} -> {val}")

    # 集計期間設定
    all_dates = [ev[0].date() for evs in ticket_events.values() for ev in evs]
    if not all_dates:
        print("ステータスイベントが見つかりませんでした。JSONに changelog を含めて取得してください。")
        return
    start_date = min(all_dates)
    end_date = datetime.now(timezone.utc).date()

    # 日次スナップショット集計
    date_counts = {}
    current = start_date
    while current <= end_date:
        snapshot = datetime.combine(current, time(0,0), tzinfo=timezone.utc)
        counts = defaultdict(int)
        if debug:
            print(f"[DEBUG] Snapshot at {snapshot.isoformat()}")
        for key, events in ticket_events.items():
            current_status = None
            if debug:
                print(f"  [DEBUG] Ticket {key}:")
            for dt, val in events:
                if dt <= snapshot:
                    current_status = val
                    if debug:
                        print(f"    [DEBUG] Matched: {dt.isoformat()} -> {val}")
                else:
                    if debug:
                        print(f"    [DEBUG] Skipped: {dt.isoformat()}")
                    break
            if debug:
                print(f"  [DEBUG] Status at {snapshot.isoformat()}: {current_status}")
            if current_status is not None:
                counts[current_status] += 1
        date_counts[current.strftime('%Y-%m-%d')] = counts
        current += timedelta(days=1)

    # 全ステータス列収集
    all_vals = sorted({v for cnts in date_counts.values() for v in cnts})

    # CSV出力
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date'] + all_vals)
        for d, cnts in date_counts.items():
            writer.writerow([d] + [cnts.get(v, 0) for v in all_vals])

    print(f"統計結果を {csv_path} に出力しました。")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='JIRA JSONから日毎カスタムフィールド集計CSVを生成')
    parser.add_argument('input_json', nargs='?', default='output.json', help='入力JSONファイル (default: output.json)')
    parser.add_argument('field_id', nargs='?', default='status', help='統計を取るフィールドID (例: status or customfield_10010)')
    parser.add_argument('--debug', action='store_true', help='デバッグログ表示')
    args = parser.parse_args()
    extract_field_counts(args.input_json, args.field_id, debug=args.debug)
