#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JIRAサーバーからチケット情報をJSON形式でダウンロードするツール

処理概要:
1. 基本設定ファイル(config.json)の存在チェック & テンプレート生成
2. フィールド設定ファイル(fields_config.json)の存在チェック & テンプレート生成
3. 両設定ファイルの読み込み
   - fields_config.json のトップレベルが {"fields": [...]} の場合、その配列を取り出す
4. チケット一覧を並列取得
   - JQL とダウンロード対象フィールドを指定
   - 各バッチ取得時に updated フィールドを強制取得
5. 各チケットのフルチェンジログを並列取得
   - .jira_cache に未加工のチェンジログ(rawHistories)をキャッシュ
   - キャッシュ内の lastUpdated と履歴対象フィールドが一致しているか判定
   - キャッシュヒット時はrawHistoriesを取り出す
6. キャッシュまたは取得した rawHistories に対して、履歴フィルタリング & field 名→ID 書き換えを実行
7. 出力ファイルに最終JSONを書き込み（トップレベルに "issues" キー）

注意点:
- curl の出力は必ず utf-8 でデコードし、errors='replace' を指定
- キャッシュディレクトリはメイン処理で一度だけ作成
"""

import os
import sys
import json
import subprocess
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

# 定数定義
CONFIG_FILE        = 'config.json'
FIELDS_FILE        = 'fields_config.json'
CACHE_DIR          = '.jira_cache'
CHUNK_SIZE         = 1000   # チケット一覧取得時の最大件数
CHANGELOG_PAGE     = 100    # 個別チェンジログ取得時の1リクエストあたり件数
DEFAULT_THREADS    = 4

# --- テンプレート生成 ---
def create_config_template(path):
    """config.json の雛形を作成して終了"""
    template = {
        "jira_url":    "https://your.jira.server",
        "username":    "your_username",
        "password":    "your_password",
        "jql":         "project = ABC ORDER BY created DESC",
        "output_file": "output.json",
        "threads":     DEFAULT_THREADS
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] 基本設定テンプレートを '{path}' に作成しました。編集後再実行してください。")


def create_fields_template(path, jira_url, auth):
    """fields_config.json の雛形を作成して終了"""
    print(f"[INFO] フィールド一覧取得: {jira_url}/rest/api/2/field")
    try:
        cmd = ['curl', '-s', '--proxy-ntlm',
               '-u', f"{auth['username']}:{auth['password']}",
               f"{jira_url.rstrip('/')}/rest/api/2/field"]
        res = subprocess.run(cmd, capture_output=True, check=True)
        raw = res.stdout.decode('utf-8', errors='replace')
        fields = json.loads(raw)
    except Exception as e:
        print(f"[ERROR] フィールド取得に失敗: {e}")
        sys.exit(1)
    template = {"fields": []}
    for fld in fields:
        template['fields'].append({
            'id': fld.get('id'),
            'name': fld.get('name'),
            'download': True if fld.get('id') in ['summary', 'status'] else False,
            'downloadHistory': True if fld.get('id') in ['summary', 'status'] else False
        })
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"[INFO] フィールド設定テンプレートを '{path}' に作成しました。編集後再実行してください。")

# --- 共通関数 ---
def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

# --- URL組み立て ---
def build_search_url(base, jql, fields, start_at, max_results):
    q_jql = quote(jql, safe='')
    q_fields = quote(fields, safe='')
    return (f"{base.rstrip('/')}/rest/api/2/search?"
            f"jql={q_jql}&fields={q_fields}&"
            f"startAt={start_at}&maxResults={max_results}")

# --- チケット一覧取得 ---
def fetch_issues(cfg, fields_param):
    print("[STEP] チケット総数取得とバッチ取得を開始")
    url0 = build_search_url(cfg['jira_url'], cfg['jql'], fields_param, 0, 1)
    r0 = subprocess.run(
        ['curl', '-s', '--proxy-ntlm', '-u', f"{cfg['username']}:{cfg['password']}", url0],
        capture_output=True
    )
    root = json.loads(r0.stdout.decode('utf-8', errors='replace'))
    total = root.get('total', 0)
    print(f"[INFO] 対象チケット総数: {total}")
    issues = []
    for start in range(0, total, CHUNK_SIZE):
        print(f" 取得 {start+1}～{min(start+CHUNK_SIZE, total)} 件目")
        urlN = build_search_url(cfg['jira_url'], cfg['jql'], fields_param, start, CHUNK_SIZE)
        rN = subprocess.run(
            ['curl', '-s', '--proxy-ntlm', '-u', f"{cfg['username']}:{cfg['password']}", urlN],
            capture_output=True
        )
        chunk = json.loads(rN.stdout.decode('utf-8', errors='replace'))
        issues.extend(chunk.get('issues', []))
    return issues

# --- フルチェンジログ取得 ---
def fetch_full_changelog(
    jira_url, auth, issue_key, updated, h_ids, h_names
):
    """
    チェンジログ(raw)を全ページ取得しキャッシュで管理
    キャッシュにヒットしたら rawHistories を返す
    """
    cache_path = os.path.join(CACHE_DIR, f"{issue_key}_changelog.json")
    # キャッシュ有効性判定: 更新日時 and historyFields
    if os.path.exists(cache_path):
        try:
            cache = load_json(cache_path)
            if cache.get('lastUpdated') == updated and cache.get('historyFields') == sorted(h_ids):
                print(f"[CACHE HIT] {issue_key} unchanged and historyFields match")
                return cache['rawHistories']
        except Exception:
            pass
    # キャッシュなし or 無効時は取得
    print(f"[STEP] Fetch raw changelog for {issue_key}")
    raw_histories = []
    start = 0
    total = None
    while True:
        url = (
            f"{jira_url.rstrip('/')}/rest/api/2/issue/{quote(issue_key)}?"
            f"expand=changelog&startAt={start}&maxResults={CHANGELOG_PAGE}"
        )
        r = subprocess.run(
            ['curl', '-s', '--proxy-ntlm', '-u', f"{auth['username']}:{auth['password']}", url],
            capture_output=True
        )
        data = json.loads(r.stdout.decode('utf-8', errors='replace'))
        cl = data.get('changelog', {})
        items = cl.get('histories', [])
        total = cl.get('total', total)
        raw_histories.extend(items)
        start += len(items)
        if start >= total:
            break
    # キャッシュ保存
    save = {'lastUpdated': updated, 'historyFields': sorted(h_ids), 'rawHistories': raw_histories}
    with open(cache_path, 'w', encoding='utf-8') as cf:
        json.dump(save, cf, indent=4, ensure_ascii=False)
    return raw_histories

# --- 履歴フィルタリング ---
def filter_and_rewrite(histories, h_ids, h_names, name_to_id):
    """
    rawHistories に対してフィルタ & field 名→ID 書き換え
    """
    result = []
    for h in histories:
        filtered = []
        for it in h.get('items', []):
            fld = it.get('field')
            if fld in h_ids or fld in h_names:
                if fld in name_to_id:
                    it['field'] = name_to_id[fld]
                filtered.append(it)
        if filtered:
            entry = h.copy()
            entry['items'] = filtered
            result.append(entry)
    return result

# --- メイン ---
def main():
    # テンプレート確認
    if not os.path.isfile(CONFIG_FILE):
        create_config_template(CONFIG_FILE)
        return
    if not os.path.isfile(FIELDS_FILE):
        cfg = load_json(CONFIG_FILE)
        create_fields_template(FIELDS_FILE, cfg['jira_url'], {'username': cfg['username'], 'password': cfg['password']})
        return
    # 設定読み込み
    cfg  = load_json(CONFIG_FILE)
    fcfg = load_json(FIELDS_FILE)
    if isinstance(fcfg, dict) and 'fields' in fcfg:
        fcfg = fcfg['fields']

    # ダウンロード対象フィールド
    download = [f['id'] for f in fcfg if f.get('download')]
    if 'updated' not in download:
        download.insert(0, 'updated')  # キャッシュ用に必須
    fields_param = ','.join(download)

    # 履歴対象
    h_ids   = {f['id'] for f in fcfg if f.get('downloadHistory')}
    h_names = {f['name'] for f in fcfg if f.get('downloadHistory')}
    name2id = {f['name']: f['id'] for f in fcfg}

    # キャッシュディレクトリ初期化
    os.makedirs(CACHE_DIR, exist_ok=True)

    # チケット一覧取得
    issues = fetch_issues(cfg, fields_param)

    # チェンジログ取得 (raw) & フィルタ
    results = {}
    with ThreadPoolExecutor(max_workers=cfg.get('threads', DEFAULT_THREADS)) as exe:
        fut_map = {}
        for issue in issues:
            key     = issue.get('key')
            updated = issue.get('fields', {}).get('updated')
            fut = exe.submit(fetch_full_changelog,
                             cfg['jira_url'], {'username':cfg['username'],'password':cfg['password']},
                             key, updated, h_ids, h_names)
            fut_map[fut] = key
        for fut, key in fut_map.items():
            raw = fut.result()
            # フィルタ & 書き換え後を格納
            results[key] = filter_and_rewrite(raw, h_ids, h_names, name2id)

    # 出力準備: トップレベルに "issues" キー
    output_data = {'issues': []}
    for issue in issues:
        key = issue.get('key')
        output_data['issues'].append({
            'key':      key,
            'fields':   issue.get('fields', {}),
            'changelog': {'histories': results.get(key, [])}
        })
    # ファイルへ書き込み
    with open(cfg.get('output_file', 'output.json'), 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    print("完了: 出力 ->", cfg.get('output_file', 'output.json'))

if __name__ == '__main__':
    main()
