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
        # JIRAサーバのベースURL
        "jira_url": "https://your.jira.server",  
        # JIRAアクセス用ユーザー情報
        "username": "your_username",
        "password": "your_password",
        # JQLクエリ
        "jql": "project = ABC",
        # 出力先ファイル名
        "output_file": "output.json",
        # 並列スレッド数
        "threads": 6,
        # ConfluenceサーバのベースURL
        "confluence_url": "https://your.confluence.server"
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] 基本設定ファイルのテンプレートを '{path}' に作成しました。内容を編集後、再実行してください。")

# フィールド設定ファイルのテンプレート作成
def create_fields_template(path, jira_url, auth):
    print(f"[INFO] フィールド一覧をJIRAサーバーから取得中: {jira_url}/rest/api/2/field")
    try:
        # curlでフィールド一覧取得
        cmd = [
            'curl', '-s', '--proxy-ntlm',
            '-u', f"{auth['username']}:{auth['password']}",
            f"{jira_url}/rest/api/2/field"
        ]
        result = subprocess.run(cmd, capture_output=True)
        data = result.stdout.decode('utf-8', errors='replace')
        fields = json.loads(data)
    except Exception as e:
        print(f"[ERROR] フィールド一覧取得に失敗しました: {e}")
        sys.exit(1)
    
    # テンプレート用設定
    template = {"fields": []}
    # デフォルトで summary と status を含め、履歴も取得する設定
    for f in fields:
        entry = {
            "id": f.get('id'),
            "name": f.get('name'),
            # フィールド自体の取得有無
            "download": True if f.get('id') in ['summary', 'status'] else False,
            # 履歴取得有無
            "downloadHistory": True if f.get('id') in ['summary', 'status'] else False
        }
        template['fields'].append(entry)
    # ファイル出力
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] フィールド設定ファイルのテンプレートを '{path}' に作成しました。内容を編集後、再実行してください。")

# JIRA APIを呼び出す関数
def fetch_issues_slice(jira_url, auth, jql, fields_param, expand_param, start_at, max_results):
    # URLクエリ部分を安全にエンコード
    encoded_jql = quote(jql, safe='')
    encoded_fields = quote(fields_param, safe='')
    url = f"{jira_url}/rest/api/2/search?jql={encoded_jql}&startAt={start_at}&maxResults={max_results}&fields={encoded_fields}"
    if expand_param:
        url += f"&expand={quote(expand_param, safe='')}"
    print(f"[INFO] チケット取得: startAt={start_at}, maxResults={max_results}")
    try:
        cmd = [
            'curl', '-s', '--proxy-ntlm',
            '-u', f"{auth['username']}:{auth['password']}",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, check=True)
        raw = result.stdout.decode('utf-8', errors='replace')
        data = json.loads(raw)
        return data
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] curlコマンド失敗: {e.returncode}, 出力: {e.output.decode('utf-8', errors='replace')}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSONデコードエラー at startAt={start_at}: {e}")
        return None

# メイン処理
def main():
    paths = get_default_paths()

    # 1. 基本設定ファイルチェック
    if not os.path.isfile(paths['config']):
        create_config_template(paths['config'])
        sys.exit(0)

    # 2. フィールド設定ファイルチェック
    if not os.path.isfile(paths['fields']):
        # コンフィグ読み込みして認証情報取得
        with open(paths['config'], encoding='utf-8') as f:
            cfg = json.load(f)
        auth = {'username': cfg['username'], 'password': cfg['password']}
        create_fields_template(paths['fields'], cfg['jira_url'], auth)
        sys.exit(0)

    # 3. 設定ファイル読み込み
    with open(paths['config'], encoding='utf-8') as f:
        cfg = json.load(f)
    with open(paths['fields'], encoding='utf-8') as f:
        fcfg = json.load(f)

    jira_url = cfg['jira_url']
    auth = {'username': cfg['username'], 'password': cfg['password']}
    jql = cfg['jql']
    output_file = cfg.get('output_file', 'output.json')
    threads = cfg.get('threads', 4)

    # 4. フィールド取得設定の整理
    download_fields = [f['id'] for f in fcfg['fields'] if f.get('download')]
    # 更新履歴取得が1つでも有効なら expand=changelog
    expand = 'changelog' if any(f.get('downloadHistory') for f in fcfg['fields']) else ''
    fields_param = ','.join(download_fields)

    # 5. 初回リクエストで総件数取得(maxResults=1)
    print("[INFO] 対象チケット総数を取得中...")
    initial = fetch_issues_slice(jira_url, auth, jql, fields_param, expand, start_at=0, max_results=1)
    if not initial:
        print("[ERROR] 初回リクエスト失敗により処理を中断します。")
        sys.exit(1)
    total = initial.get('total', 0)
    print(f"[INFO] 対象チケット総数: {total}")

    max_per_request = 1000
    offsets = list(range(0, total, max_per_request))

    # 6. 並列取得
    all_issues = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(fetch_issues_slice, jira_url, auth, jql, fields_param, expand, start, max_per_request): start
            for start in offsets
        }
        for future in as_completed(futures):
            start = futures[future]
            data = future.result()
            if data and 'issues' in data:
                print(f"[INFO] startAt={start} 取得 {len(data['issues'])} 件のチケット")
                all_issues.extend(data['issues'])
            else:
                print(f"[WARN] startAt={start} のデータ取得に問題があります。")

    # 7. フィルタリング: 必要であれば履歴アイテムをフィルタ
    if expand:
        print("[INFO] 履歴フィルタリングを実行します...")
        history_fields = {f['id'] for f in fcfg['fields'] if f.get('downloadHistory')}
        for issue in all_issues:
            if 'changelog' in issue:
                original = issue['changelog'].get('histories', [])
                filtered = []
                for hist in original:
                    # 各履歴アイテム内の items をチェック
                    items = [item for item in hist.get('items', []) if item.get('field') in history_fields]
                    if items:
                        hist['items'] = items
                        filtered.append(hist)
                issue['changelog']['histories'] = filtered

    # 8. 出力
    print(f"[INFO] 全件取得完了: {len(all_issues)} 件。ファイル '{output_file}' に保存します。")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({ 'issues': all_issues }, f, indent=4, ensure_ascii=False)
        print("[INFO] 正常に保存しました。終了します。")
    except Exception as e:
        print(f"[ERROR] 出力ファイル保存中にエラーが発生: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
