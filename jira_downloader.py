#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JIRAサーバーからチケット情報をJSON形式でダウンロードするツール

動作概要:
1. 基本設定ファイル(config.json)の存在チェック
   - 存在しない場合、テンプレートを作成して終了
2. フィールド設定ファイル(fields_config.json)の存在チェック
   - 存在しない場合、JIRAサーバーからフィールド一覧を取得してテンプレートを作成し終了
3. 設定ファイル読み込み
4. チケット総数取得
5. 並列でチケット一覧取得
6. 各チケットのフルチェンジログを並列取得（100件制限を回避）
7. 履歴フィルタリング
8. 最終JSONファイルとして保存

※基本設定ファイルとフィールド設定ファイルはローカルに保存し、Git管理から除外してください。
"""
import os
import sys
import json
import subprocess
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 定数定義 ---
CONFIG_FILE = 'config.json'
FIELDS_FILE = 'fields_config.json'
CHUNK_SIZE = 1000    # チケット一覧取得時の最大件数
CHANGELOG_PAGE = 100  # 個別チェンジログ取得時の1リクエストあたり件数

# --- テンプレート生成関数 ---

def create_config_template(path):
    """
    基本設定ファイル(config.json)のテンプレートを生成する
    - jira_url: JIRAのベースURL
    - username/password: 認証情報
    - jql: ダウンロード対象チケットを指定するJQL
    - output_file: 出力ファイル名
    - threads: 並列スレッド数
    """
    template = {
        "jira_url": "https://your.jira.server",  # JIRAサーバーのURL
        "username": "your_username",             # アクセス用ユーザー名
        "password": "your_password",             # アクセス用パスワード
        "jql": "project = ABC",                 # ダウンロード対象JQL
        "output_file": "output.json",            # 出力ファイル名
        "threads": 4                               # 並列スレッド数
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] 基本設定ファイルテンプレートを '{path}' に作成しました。内容を編集後、再実行してください。")


def create_fields_template(path, jira_url, auth):
    """
    フィールド設定ファイル(fields_config.json)のテンプレートを生成する
    - JIRAサーバーからフィールド一覧を取得
    - デフォルトで summary と status をダウンロード対象＆履歴取得対象に設定
    """
    print(f"[INFO] フィールド一覧を取得しています: {jira_url}/rest/api/2/field")
    try:
        cmd = [
            'curl', '-s', '--proxy-ntlm',
            '-u', f"{auth['username']}:{auth['password']}",
            f"{jira_url}/rest/api/2/field"
        ]
        result = subprocess.run(cmd, capture_output=True, check=True)
        fields = json.loads(result.stdout.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"[ERROR] フィールド取得失敗: {e}")
        sys.exit(1)

    # テンプレート作成
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
    print(f"[INFO] フィールド設定ファイルテンプレートを '{path}' に作成しました。内容を編集後、再実行してください。")

# --- JIRA API 呼び出し ---

def fetch_issues_slice(jira_url, auth, jql, fields_param, start_at, max_results):
    """
    チケット一覧(部分)を取得する
    start_at: 取得開始オフセット
    max_results: 取得件数
    """
    # パラメータURLエンコード
    e_jql = quote(jql, safe='')
    e_fields = quote(fields_param, safe='')
    url = f"{jira_url}/rest/api/2/search?jql={e_jql}&startAt={start_at}&maxResults={max_results}&fields={e_fields}"
    cmd = ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", url]
    try:
        res = subprocess.run(cmd, capture_output=True, check=True)
        return json.loads(res.stdout.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"[ERROR] fetch_issues_slice エラー: {e}")
        return None


def fetch_full_changelog(jira_url, auth, issue_key):
    """
    単一課題のフルチェンジログを取得 (100件制限をページングで回避)
    """
    all_histories = []
    start_at = 0
    total = None
    while True:
        e_key = quote(issue_key, safe='')
        url = f"{jira_url}/rest/api/2/issue/{e_key}?expand=changelog&startAt={start_at}&maxResults={CHANGELOG_PAGE}"
        cmd = ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", url]
        try:
            res = subprocess.run(cmd, capture_output=True, check=True)
            data = json.loads(res.stdout.decode('utf-8', errors='replace'))
            changelog = data.get('changelog', {})
            histories = changelog.get('histories', [])
            if total is None:
                total = changelog.get('total', 0)
        except Exception as e:
            print(f"[ERROR] fetch_full_changelog ({issue_key}) エラー: {e}")
            break
        # 履歴が空なら終了
        if not histories:
            break
        all_histories.extend(histories)
        start_at += len(histories)
        # 取得済みが総数以上なら終了
        if total is not None and start_at >= total:
            break
    return all_histories

# --- メイン処理 ---

def main():
    # 1. 設定ファイルチェック
    if not os.path.isfile(CONFIG_FILE):
        create_config_template(CONFIG_FILE)
        sys.exit(0)
    if not os.path.isfile(FIELDS_FILE):
        cfg = json.load(open(CONFIG_FILE, encoding='utf-8'))
        create_fields_template(FIELDS_FILE, cfg['jira_url'], {'username': cfg['username'], 'password': cfg['password']})
        sys.exit(0)

    # 2. 設定読み込み
    cfg = json.load(open(CONFIG_FILE, encoding='utf-8'))
    fcfg = json.load(open(FIELDS_FILE, encoding='utf-8'))
    jira_url = cfg['jira_url']
    auth = {'username': cfg['username'], 'password': cfg['password']}
    jql = cfg['jql']
    output_file = cfg.get('output_file', 'output.json')
    threads = cfg.get('threads', 4)

    # 3. ダウンロード対象フィールド準備
    download_fields = [f['id'] for f in fcfg['fields'] if f.get('download')]
    fields_param = ','.join(download_fields)

    # 4. チケット総数取得
    print("[INFO] チケット総数を取得中...")
    initial = fetch_issues_slice(jira_url, auth, jql, fields_param, 0, 1)
    total = initial.get('total', 0) if initial else 0
    print(f"[INFO] 対象チケット総数: {total}")

    # 5. チケット一覧並列取得
    offsets = list(range(0, total, CHUNK_SIZE))
    all_issues = []
    print("[INFO] チケット一覧の並列取得を開始... ")
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(fetch_issues_slice, jira_url, auth, jql, fields_param, off, CHUNK_SIZE): off for off in offsets}
        for fut in as_completed(futures):
            data = fut.result()
            if data and 'issues' in data:
                issues = data['issues']
                all_issues.extend(issues)
                print(f"[INFO] startAt={futures[fut]} 取得 {len(issues)} 件")

    # 6. フルチェンジログ並列取得
    print("[INFO] フルチェンジログの並列取得を開始... ")
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_map = {executor.submit(fetch_full_changelog, jira_url, auth, issue.get('key') or issue.get('id')): issue for issue in all_issues}
        for fut in as_completed(future_map):
            issue = future_map[fut]
            histories = fut.result() or []
            issue['changelog'] = {'histories': histories}
            print(f"[INFO] {issue.get('key')} のチェンジログ {len(histories)} 件取得完了")

    # 7. 履歴フィルタリング
    history_ids = {f['id'] for f in fcfg['fields'] if f.get('downloadHistory')}
    history_names = {f['name'] for f in fcfg['fields'] if f.get('downloadHistory')}
    print("[INFO] 履歴フィルタリングを実行... ")
    for issue in all_issues:
        filtered = []
        for hist in issue.get('changelog', {}).get('histories', []):
            items = [it for it in hist.get('items', []) if it.get('fieldId') in history_ids or it.get('field') in history_names]
            if items:
                hist['items'] = items
                filtered.append(hist)
        issue['changelog']['histories'] = filtered

    # 8. 出力
    print(f"[INFO] 全件取得完了: {len(all_issues)} 件。ファイル '{output_file}' に保存します。")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({'issues': all_issues}, f, indent=4, ensure_ascii=False)
        print("[INFO] 正常に保存しました。終了します。")
    except Exception as e:
        print(f"[ERROR] ファイル保存中にエラー発生: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
