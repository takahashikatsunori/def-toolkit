#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JIRAサーバーからチケット情報をJSON形式でダウンロードするツール

動作概要:
1. 基本設定ファイル(config.json)の存在チェック
2. フィールド設定ファイル(fields_config.json)の存在チェック
3. 設定ファイル読み込み
4. チケット総数取得
5. 並列でチケット一覧取得（`updated` フィールドを含める）
6. 各チケットのフルチェンジログを並列取得、ローカルにキャッシュ
   - キャッシュ内に保存した `lastUpdated` と現在の `updated` を比較し、一致すればキャッシュを利用
   - 不一致またはキャッシュなしの場合は再取得
7. 履歴フィルタリング（field項目をIDに書き換え）
8. 最終JSONファイルとして保存

※キャッシュは .jira_cache ディレクトリに保存されます。Git管理から除外してください。
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
CHUNK_SIZE = 1000          # チケット一覧取得時の最大件数
CHANGELOG_PAGE = 100       # 個別チェンジログ取得時の1リクエストあたり件数
CACHE_DIR = '.jira_cache'  # キャッシュ保存ディレクトリ

# --- キャッシュディレクトリ作成 ---
def ensure_cache_dir():
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR)
        print(f"[INFO] キャッシュディレクトリ '{CACHE_DIR}' を作成しました。")

# --- テンプレート生成関数 ---
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
    print(f"[INFO] 基本設定ファイルテンプレートを '{path}' に作成しました。内容を編集後、再実行してください。")


def create_fields_template(path, jira_url, auth):
    print(f"[INFO] フィールド一覧を取得しています: {jira_url}/rest/api/2/field")
    try:
        cmd = ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", f"{jira_url}/rest/api/2/field"]
        result = subprocess.run(cmd, capture_output=True, check=True)
        fields = json.loads(result.stdout.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"[ERROR] フィールド取得失敗: {e}")
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
    print(f"[INFO] フィールド設定ファイルテンプレートを '{path}' に作成しました。内容を編集後、再実行してください。")

# --- JIRA API 呼び出し ---
def fetch_issues_slice(jira_url, auth, jql, fields_param, start_at, max_results):
    """チケット一覧(部分)を取得 (必ず `updated` フィールドを含める)"""
    # fields_param に updated を追加
    if 'updated' not in fields_param.split(','):
        fields_param = 'updated,' + fields_param
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

# --- フルチェンジログ取得（更新日時チェック付きキャッシュ） ---
def fetch_full_changelog(jira_url, auth, issue_key, issue_updated):
    ensure_cache_dir()
    cache_file = os.path.join(CACHE_DIR, f"{issue_key}_changelog.json")
    # キャッシュが存在し、保存された lastUpdated が現在の issue_updated と一致する場合
    if os.path.isfile(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as cf:
                cached = json.load(cf)
            if cached.get('lastUpdated') == issue_updated:
                print(f"[INFO] キャッシュ有効: '{issue_key}' (updated={issue_updated})")
                return cached.get('histories', [])
            else:
                print(f"[INFO] 更新検知: '{issue_key}' updated changed ({cached.get('lastUpdated')} -> {issue_updated}) 再取得します。")
        except Exception as e:
            print(f"[WARN] キャッシュ読み込み失敗({issue_key}): {e} - 再取得します。")
    # 再取得
    all_histories = []
    start_at = 0
    total = None
    while True:
        e_key = quote(issue_key, safe='')
        url = f"{jira_url}/rest/api/2/issue/{e_key}?expand=changelog&startAt={start_at}&maxResults={CHANGELOG_PAGE}"
        cmd = ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", url]
        try:
            res = subprocess.run(cmd, capture_output=True, check=True)
            info = json.loads(res.stdout.decode('utf-8', errors='replace'))
            changelog = info.get('changelog', {})
            histories = changelog.get('histories', [])
            if total is None:
                total = changelog.get('total', 0)
        except Exception as e:
            print(f"[ERROR] fetch_full_changelog ({issue_key}) エラー: {e}")
            break
        if not histories:
            break
        all_histories.extend(histories)
        start_at += len(histories)
        if total is not None and start_at >= total:
            break
    # キャッシュに保存 (lastUpdated と histories をセット)
    try:
        with open(cache_file, 'w', encoding='utf-8') as cf:
            json.dump({'lastUpdated': issue_updated, 'histories': all_histories}, cf, indent=4, ensure_ascii=False)
        print(f"[INFO] '{issue_key}' のチェンジログをキャッシュに保存しました(updated={issue_updated})")
    except Exception as e:
        print(f"[WARN] キャッシュ保存失敗({issue_key}): {e}")
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
        futures = {}
        for off in offsets:
            futures[executor.submit(fetch_issues_slice, jira_url, auth, jql, fields_param, off, CHUNK_SIZE)] = off
        for fut, off in futures.items():
            data = fut.result()
            if data and 'issues' in data:
                all_issues.extend(data['issues'])
                print(f"[INFO] startAt={off} 取得 {len(data['issues'])} 件")

    # 6. フルチェンジログ並列取得
    print("[INFO] フルチェンジログの並列取得を開始... ")
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {}
        for issue in all_issues:
            # fields.updated 取得
            issue_updated = issue.get('fields', {}).get('updated')
            futures[executor.submit(fetch_full_changelog, jira_url, auth, issue.get('key') or issue.get('id'), issue_updated)] = issue
        for fut, issue in futures.items():
            histories = fut.result() or []
            issue['changelog'] = {'histories': histories}
            print(f"[INFO] {issue.get('key')} のチェンジログ {len(histories)} 件取得完了")

    # 7. 履歴フィルタリングとfield書き換え
    history_ids = {f['id'] for f in fcfg['fields'] if f.get('downloadHistory')}
    history_names = {f['name'] for f in fcfg['fields'] if f.get('downloadHistory')}
    name_to_id = {f['name']: f['id'] for f in fcfg['fields']}
    print("[INFO] 履歴フィルタリングとfield書き換えを実行... ")
    for issue in all_issues:
        filtered = []
        for hist in issue.get('changelog', {}).get('histories', []):
            new_items = []
            for it in hist.get('items', []):
                fv = it.get('field')
                if fv in history_ids or fv in history_names:
                    if fv in name_to_id:
                        it['field'] = name_to_id[fv]
                    new_items.append(it)
            if new_items:
                hist['items'] = new_items
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
