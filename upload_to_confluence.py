import os
import sys
import json
import subprocess

def load_config(config_path='config.json'):
    if not os.path.exists(config_path):
        # 共通テンプレート出力
        template = {
            "jira_url": "https://your.jira.server",
            "username": "your_username",
            "password": "your_password",
            "jql": "project = ABC",
            "output_file": "output.json",
            "threads": 4,
            "confluence_url": "https://your.confluence.server"
        }
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(template, f, indent=4)
        print(f"設定ファイル '{config_path}' を作成しました。内容を編集してください。")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def check_existing_attachment(file_name, page_id, config):
    url = config['confluence_url'].rstrip('/')
    user = config['username']
    password = config['password']

    api_url = f"{url}/rest/api/content/{page_id}/child/attachment?filename={file_name}&expand=version"

    command = [
        "curl",
        "--proxy-ntlm",
        "-u", f"{user}:{password}",
        "-X", "GET",
        api_url
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        print("添付ファイル確認失敗...")
        print("stderr:", result.stderr)
        print("stdout:", result.stdout)
        sys.exit(1)

    response = json.loads(result.stdout)
    results = response.get('results', [])

    if results:
        return results[0]['id']  # 最初に見つかった添付ファイルのIDを返す
    else:
        return None

def upload_attachment(file_path, page_id, config):
    url = config['confluence_url'].rstrip('/')
    user = config['username']
    password = config['password']

    file_name = os.path.basename(file_path)
    existing_attachment_id = check_existing_attachment(file_name, page_id, config)

    if existing_attachment_id:
        print(f"既存ファイルが見つかりました。ID: {existing_attachment_id} → 更新します。")
        api_url = f"{url}/rest/api/content/{existing_attachment_id}/data"
    else:
        print("既存ファイルは見つかりませんでした。新規追加します。")
        api_url = f"{url}/rest/api/content/{page_id}/child/attachment"

    # --proxy-ntlm オプションを使用してcurl実行
    command = [
        "curl",
        "--proxy-ntlm",
        "-u", f"{user}:{password}",
        "-X", "POST",
        f"{api_url}?notifyWatchers=false",
        "-F", f"file=@{file_path}"
    ]

    print(f"実行コマンド: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        print("アップロード成功！")
    else:
        print("アップロード失敗...")
        print("stderr:", result.stderr)
        print("stdout:", result.stdout)
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("使い方: python upload_to_confluence.py <ファイルパス> <ページID>")
        sys.exit(1)

    file_path = sys.argv[1]
    page_id = sys.argv[2]

    if not os.path.exists(file_path):
        print(f"エラー: ファイル '{file_path}' が見つかりません。")
        sys.exit(1)

    config = load_config()
    upload_attachment(file_path, page_id, config)

if __name__ == "__main__":
    main()