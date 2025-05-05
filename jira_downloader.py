#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JIRA チケット情報一括ダウンロードツール

- config.json に JIRA サーバ情報・JQL・出力先・スレッド数を記載
- fields_config.json にダウンロード対象フィールドと履歴取得フラグを記載
- 初回実行時にテンプレートを出力して終了
- チケット一覧を並列取得し、各チケットごとにフルチェンジログを並列取得
- .jira_cache にフルチェンジログをキャッシュし、updated フィールドで妥当性をチェック
- changelog 内の field は常にフィールドIDに書き換え
"""

import os
import sys
import json
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# 定数
CONFIG_FILE         = 'config.json'
FIELDS_CONFIG_FILE  = 'fields_config.json'
CACHE_DIR           = '.jira_cache'
DEFAULT_MAX_RESULTS = 1000

def generate_config_template():
    """config.json のテンプレートを生成"""
    tmpl = {
        "jira_url":    "https://your-jira-server",
        "username":    "your-username",
        "password":    "your-password",
        "jql":         "project = YOURPROJECT ORDER BY created DESC",
        "output_file": "output.json",
        "threads":     4,
        "max_results": DEFAULT_MAX_RESULTS
    }
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(tmpl, f, ensure_ascii=False, indent=4)
    print(f"テンプレート {CONFIG_FILE} を生成しました。設定を入力して再実行してください。")

def generate_fields_config_template(config):
    """fields_config.json のテンプレートを生成"""
    url = f"{config['jira_url'].rstrip('/')}/rest/api/2/field"
    cmd = [
        'curl', '-s', '--proxy-ntlm',
        '-u', f"{config['username']}:{config['password']}",
        url
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("フィールド一覧取得エラー:", res.stderr, file=sys.stderr)
        sys.exit(1)

    all_fields = json.loads(res.stdout)
    tmpl = []
    for fld in all_fields:
        entry = {
            "id":             fld.get('id'),
            "name":           fld.get('name'),
            "download":       False,
            "downloadHistory": False
        }
        # summary と status はデフォルトで有効
        if fld.get('id') in ('summary', 'status'):
            entry['download']       = True
            entry['downloadHistory'] = True
        tmpl.append(entry)

    with open(FIELDS_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(tmpl, f, ensure_ascii=False, indent=4)
    print(f"テンプレート {FIELDS_CONFIG_FILE} を生成しました。設定を入力して再実行してください。")

def load_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def build_search_url(jira_url, jql, fields_param, start_at, max_results, expand):
    """チケット検索用 URL を組み立て"""
    q = quote(jql, safe='')
    fs = quote(fields_param, safe='')
    url = (
        f"{jira_url.rstrip('/')}/rest/api/2/search"
        f"?jql={q}&fields={fs}"
        f"&startAt={start_at}&maxResults={max_results}"
    )
    if expand:
        url += f"&expand={expand}"
    return url

def fetch_issues(config, fields_param):
    """チケット一覧をバッチで取得"""
    print("== チケット一覧取得 ==")
    # まず total 件数のみ取得
    url1 = build_search_url(
        config['jira_url'],
        config['jql'],
        fields_param,
        start_at=0,
        max_results=1,
        expand=''
    )
    cmd1 = ['curl','-s','--proxy-ntlm',
            '-u',f"{config['username']}:{config['password']}",
            url1]
    r1 = subprocess.run(cmd1, capture_output=True, text=True)
    root = json.loads(r1.stdout)
    total = root.get('total', 0)
    print(f"総件数: {total} 件")

    issues = []
    batch_size = config.get('max_results', DEFAULT_MAX_RESULTS)
    for start in range(0, total, batch_size):
        print(f"  {start+1} ～ {min(start+batch_size, total)} 件目取得...")
        urlN = build_search_url(
            config['jira_url'],
            config['jql'],
            fields_param,
            start_at=start,
            max_results=batch_size,
            expand=''  # changelog は個別取得
        )
        cmdN = ['curl','-s','--proxy-ntlm',
                '-u',f"{config['username']}:{config['password']}",
                urlN]
        rN = subprocess.run(cmdN, capture_output=True, text=True)
        batch = json.loads(rN.stdout).get('issues', [])
        issues.extend(batch)
    return issues

def fetch_full_changelog(issue, config, history_ids, history_names, name_to_id):
    """
    チケットのフルチェンジログを取得し、キャッシュと比較。
    更新なしならキャッシュを返却。更新ありorキャッシュなしなら再取得。
    """
    key     = issue.get('key')
    updated = issue.get('fields', {}).get('updated')
    cache_path = os.path.join(CACHE_DIR, f"{key}.json")

    # キャッシュ有効チェック
    if os.path.exists(cache_path):
        try:
            cache = load_json(cache_path)
            if cache.get('lastUpdated') == updated:
                print(f"[キャッシュ] {key} (更新なし)")
                return cache['changelog']
        except Exception as e:
            print(f"[キャッシュ読込失敗] {key}: {e}")

    print(f"[取得開始] {key} のフルチェンジログ...")
    all_histories = []
    start_at = 0

    while True:
        url = (
            f"{config['jira_url'].rstrip('/')}/rest/api/2/issue/{quote(key)}"
            f"?expand=changelog&startAt={start_at}"
        )
        cmd = ['curl','-s','--proxy-ntlm',
               '-u',f"{config['username']}:{config['password']}",
               url]
        res = subprocess.run(cmd, capture_output=True, text=True)
        data = json.loads(res.stdout)
        changelog = data.get('changelog', {})
        histories = changelog.get('histories', [])
        total     = changelog.get('total', 0)

        # filter & rewrite field → always ID
        for hist in histories:
            items = []
            for it in hist.get('items', []):
                fld = it.get('field')
                if fld in history_ids:
                    items.append(it)
                elif fld in history_names:
                    # name→ID へ書き換え
                    it['field'] = name_to_id.get(fld, fld)
                    items.append(it)
            if items:
                new_hist = hist.copy()
                new_hist['items'] = items
                all_histories.append(new_hist)

        start_at += len(histories)
        if start_at >= total:
            break

    full = {'histories': all_histories}
    # キャッシュ書き込み
    save_json(cache_path, {'lastUpdated': updated, 'changelog': full})
    return full

def main():
    parser = argparse.ArgumentParser(description='JIRA チケット JSON 一括ダウンロード')
    parser.add_argument('--config', default=CONFIG_FILE, help='基本設定ファイル')
    parser.add_argument('--fields', default=FIELDS_CONFIG_FILE, help='フィールド設定ファイル')
    args = parser.parse_args()

    # --- 設定ファイルチェック ---
    if not os.path.exists(args.config):
        generate_config_template()
        sys.exit(0)
    config = load_json(args.config)

    if not os.path.exists(args.fields):
        generate_fields_config_template(config)
        sys.exit(0)
    fields_conf = load_json(args.fields)

    # --- 対象フィールドと履歴対象を抽出 ---
    download_fields  = [f['id']   for f in fields_conf if f.get('download')]
    history_ids      = [f['id']   for f in fields_conf if f.get('downloadHistory')]
    history_names    = [f['name'] for f in fields_conf if f.get('downloadHistory')]
    name_to_id       = {f['name']: f['id'] for f in fields_conf}

    fields_param = ','.join(download_fields)

    # --- キャッシュディレクトリはメインスレッドで一度だけ作成 ---
    os.makedirs(CACHE_DIR, exist_ok=True)

    # --- チケット一覧取得 ---
    issues = fetch_issues(config, fields_param)

    # --- フルチェンジログを並列取得 ---
    threads = config.get('threads', 4)
    all_changelogs = {}
    with ThreadPoolExecutor(max_workers=threads) as exe:
        fut_map = {
            exe.submit(fetch_full_changelog, issue, config,
                       history_ids, history_names, name_to_id): issue.get('key')
            for issue in issues
        }
        for fut in as_completed(fut_map):
            key = fut_map[fut]
            try:
                all_changelogs[key] = fut.result()
            except Exception as e:
                print(f"[Error] {key}: {e}", file=sys.stderr)

    # --- 最終出力JSON作成 ---
    output = []
    for issue in issues:
        key = issue.get('key')
        output.append({
            'key':     key,
            'fields':  issue.get('fields', {}),
            'changelog': all_changelogs.get(key, {})
        })

    save_json(config.get('output_file', 'output.json'), output)
    print("完了: 出力ファイル ->", config.get('output_file', 'output.json'))

if __name__ == '__main__':
    main()
