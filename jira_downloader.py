#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JIRAサーバーからチケット情報をJSON形式でダウンロードするツール

動作概要:
1. 基本設定ファイル(config.json)の存在チェック
   - 存在しない場合、テンプレートを作成して終了
2. フィールド設定ファイル(fields_config.json)の存在チェック
   - 存在しない場合、JIRAサーバーからフィールド一覧を取得し、テンプレートを作成して終了
3. 設定ファイルを読み込み、JQL・取得フィールド・履歴取得設定を取得
4. 初回リクエストで対象チケット総数を取得し、必要な分割オフセットを計算
5. ThreadPoolExecutorにより並列でcurlを実行し、各ページを取得
6. 取得結果(JSON)をパースし、ダウンロード対象フィールドと履歴をフィルタリング
   - フル履歴取得を行い、100件制限を回避（無限ループ防止）
7. 全件を統合し、最終的な1つのJSONファイル(output.json)として保存
8. 各ステップで詳細なログを標準出力に出力し、エラー発生時は可能な限り情報を表示
"""
import os
import sys
import json
import subprocess
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定ファイル名定義
def get_default_paths():
    return {
        'config': 'config.json',
        'fields': 'fields_config.json'
    }

# 基本設定ファイルのテンプレート作成
def create_config_template(path):
    template = {
        "jira_url": "https://your.jira.server",
        "username": "your_username",
        "password": "your_password",
        "jql": "project = ABC",
        "output_file": "output.json",
        "threads": 4
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] 基本設定ファイルのテンプレートを '{path}' に作成しました。編集後、再実行してください。")

# フィールド設定ファイルのテンプレート作成
def create_fields_template(path, jira_url, auth):
    print(f"[INFO] フィールド一覧を取得中: {jira_url}/rest/api/2/field")
    try:
        cmd = [
            'curl', '-s', '--proxy-ntlm',
            '-u', f"{auth['username']}:{auth['password']}",
            f"{jira_url}/rest/api/2/field"
        ]
        result = subprocess.run(cmd, capture_output=True, check=True)
        fields = json.loads(result.stdout.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"[ERROR] フィールド取得に失敗: {e}")
        sys.exit(1)

    template = {"fields": []}
    for f in fields:
        template['fields'].append({
            "id": f.get('id'),
            "name": f.get('name'),
            "download": True if f.get('id') in ['summary', 'status'] else False,
            "downloadHistory": True if f.get('id') in ['summary', 'status'] else False
        })
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] フィールド設定ファイルのテンプレートを '{path}' に作成しました。編集後、再実行してください。")

# チケット検索API呼び出し
def fetch_issues_slice(jira_url, auth, jql, fields_param, start_at, max_results):
    encoded_jql = quote(jql, safe='')
    encoded_fields = quote(fields_param, safe='')
    url = f"{jira_url}/rest/api/2/search?jql={encoded_jql}&startAt={start_at}&maxResults={max_results}&fields={encoded_fields}"
    print(f"[INFO] チケット取得: startAt={start_at}, maxResults={max_results}")
    try:
        cmd = ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", url]
        res = subprocess.run(cmd, capture_output=True, check=True)
        return json.loads(res.stdout.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"[ERROR] fetch_issues_slice failed: {e}")
        return None

# 個別課題のフルチェンジログ取得 (100件超も取得、無限ループ防止)
def fetch_full_changelog(jira_url, auth, issue_key):
    all_histories = []
    start_at = 0
    max_chg = 100
    total = None
    while True:
        url = (f"{jira_url}/rest/api/2/issue/{quote(issue_key, safe='')}"
               f"?expand=changelog&startAt={start_at}&maxResults={max_chg}")
        print(f"[INFO] フルチェンジログ取得: {issue_key} startAt={start_at}")
        try:
            cmd = ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", url]
            res = subprocess.run(cmd, capture_output=True, check=True)
            data = json.loads(res.stdout.decode('utf-8', errors='replace'))
            changelog = data.get('changelog', {})
            histories = changelog.get('histories', [])
            if total is None:
                total = changelog.get('total', 0)
        except Exception as e:
            print(f"[ERROR] fetch_full_changelog failed for {issue_key}: {e}")
            break
        if not histories:
            print(f"[INFO] {issue_key} のチェンジログ取得終了 (empty)")
            break
        all_histories.extend(histories)
        start_at += len(histories)
        # 総件数が取得済みなら、取得済み開始位置が総数以上で終了
        if total is not None and start_at >= total:
            print(f"[INFO] {issue_key} のチェンジログ取得終了 (reached total={total})")
            break
    return all_histories

# メイン処理
def main():
    paths = get_default_paths()
    # 1,2. 設定ファイルチェック
    if not os.path.isfile(paths['config']): create_config_template(paths['config']); sys.exit(0)
    if not os.path.isfile(paths['fields']):
        cfg = json.load(open(paths['config'], encoding='utf-8'))
        create_fields_template(paths['fields'], cfg['jira_url'], {'username':cfg['username'],'password':cfg['password']})
        sys.exit(0)

    cfg = json.load(open(paths['config'], encoding='utf-8'))
    fcfg = json.load(open(paths['fields'], encoding='utf-8'))
    jira_url = cfg['jira_url']
    auth = {'username':cfg['username'], 'password':cfg['password']}
    jql = cfg['jql']
    output_file = cfg.get('output_file','output.json')
    threads = cfg.get('threads',4)
    download_fields = [f['id'] for f in fcfg['fields'] if f.get('download')]
    fields_param = ','.join(download_fields)

    print("[INFO] 対象チケット総数を取得中...")
    initial = fetch_issues_slice(jira_url, auth, jql, fields_param, 0, 1)
    total = initial.get('total',0) if initial else 0
    print(f"[INFO] 対象チケット総数: {total}")

    offsets = list(range(0, total, 1000))
    all_issues = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(fetch_issues_slice, jira_url, auth, jql, fields_param, off, 1000):off for off in offsets}
        for fut in as_completed(futures):
            data = fut.result()
            if data and 'issues' in data:
                all_issues.extend(data['issues'])
                print(f"[INFO] startAt={futures[fut]} 取得 {len(data['issues'])} 件")

    # フルチェンジログ取得
    print("[INFO] 全課題のフルチェンジログ取得を開始します...")
    for idx, issue in enumerate(all_issues,1):
        key = issue.get('key') or issue.get('id')
        histories = fetch_full_changelog(jira_url, auth, key)
        issue['changelog'] = {'histories': histories}
        print(f"[INFO] {idx}/{len(all_issues)} {key} のチェンジログ {len(histories)} 件取得完了")

    # 履歴フィルタリング
    print("[INFO] 履歴フィルタリング実行...")
    history_ids = {f['id'] for f in fcfg['fields'] if f.get('downloadHistory')}
    history_names = {f['name'] for f in fcfg['fields'] if f.get('downloadHistory')}
    for issue in all_issues:
        filtered = []
        for hist in issue['changelog']['histories']:
            items = [it for it in hist.get('items',[])
                     if it.get('fieldId') in history_ids or it.get('field') in history_names]
            if items:
                hist['items'] = items
                filtered.append(hist)
        issue['changelog']['histories'] = filtered

    # 出力
    with open(output_file,'w',encoding='utf-8') as f:
        json.dump({'issues':all_issues}, f, indent=4, ensure_ascii=False)
    print(f"[INFO] 保存完了: {output_file} (全{len(all_issues)}件)")

if __name__ == '__main__':
    main()
