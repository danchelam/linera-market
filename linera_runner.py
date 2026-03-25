"""
Linera Prediction Market — 启动器 + Web 控制台
───────────────────────────────────────────────
功能：
  1. 启动时从 GitHub 自动检查 / 下载最新 linera_task.py & base_module.py & linera_runner.py
  2. 动态加载外部脚本（热更新：替换 .py 即可，无需重新打包 exe）
  3. Flask + SocketIO Web 面板控制任务启停、查看日志
  4. 打包为 exe 后，业务逻辑全部通过外部 .py 文件加载
  5. linera_runner.py 自身热更新后自动重启
"""

__version__ = "2026.03.25.5"

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import subprocess
import threading
import asyncio
import time
import os
import sys
import importlib.util
import re
import json
import urllib.request
import urllib.error

# ═══════════════════════════════════════════════
#  路径工具
# ═══════════════════════════════════════════════

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


# ═══════════════════════════════════════════════
#  Flask 应用初始化
# ═══════════════════════════════════════════════

template_dir = os.path.join(get_base_dir(), "templates")
if not os.path.exists(template_dir):
    template_dir = get_resource_path(os.path.join("Linera", "templates"))
if not os.path.exists(template_dir):
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

app = Flask(__name__, template_folder=template_dir)
app.config['SECRET_KEY'] = 'linera-secret'
socketio = SocketIO(app, async_mode='threading')

# ═══════════════════════════════════════════════
#  自动更新配置 — 替换为你自己的 GitHub Raw URL
# ═══════════════════════════════════════════════

CHECK_UPDATE_ON_START = True

# 状态上报地址（tasks_manager），Runner 会每 2 秒推送一次状态
REPORT_URL = "http://100.103.90.123:8888/api/linera_report"
RUNNER_NAME = os.environ.get("RUNNER_NAME", "")

_GH_RAW = "https://raw.githubusercontent.com/danchelam/linera-market/refs/heads/main"
_CDN_RAW = "https://cdn.jsdelivr.net/gh/danchelam/linera-market@main"
UPDATE_META_URL = f"{_GH_RAW}/version.json"
UPDATE_TASK_URL = f"{_GH_RAW}/linera_task.py"
UPDATE_BASE_URL = f"{_GH_RAW}/base_module.py"
UPDATE_RUNNER_URL = f"{_GH_RAW}/linera_runner.py"
_CDN_META_URL = f"{_CDN_RAW}/version.json"
_CDN_TASK_URL = f"{_CDN_RAW}/linera_task.py"
_CDN_BASE_URL = f"{_CDN_RAW}/base_module.py"
_CDN_RUNNER_URL = f"{_CDN_RAW}/linera_runner.py"

LAST_TASK_VERSION = "0"
LAST_BASE_VERSION = "0"
LAST_RUNNER_VERSION = __version__
LAST_REMOTE_TASK_VERSION = ""
LAST_REMOTE_BASE_VERSION = ""
LAST_REMOTE_RUNNER_VERSION = ""
LAST_UPDATE_STATUS = "unknown"

# ═══════════════════════════════════════════════
#  版本读取 / 比较 / 更新
# ═══════════════════════════════════════════════

def read_local_version(script_path: str) -> str:
    if not os.path.exists(script_path):
        return "0"
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read(4096)
        m = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", content)
        return m.group(1) if m else "0"
    except Exception:
        return "0"


def parse_version(v: str):
    nums = re.findall(r"\d+", v)
    return tuple(int(x) for x in nums) if nums else (0,)


def _url_fetch(url: str, timeout: int = 15) -> str:
    """下载 URL 内容，返回文本；失败返回空字符串"""
    ts = int(time.time())
    full = f"{url}{'&' if '?' in url else '?'}t={ts}"
    with urllib.request.urlopen(full, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def fetch_remote_versions() -> dict:
    if not UPDATE_META_URL:
        return {}
    for label, meta_url in [("GitHub", UPDATE_META_URL), ("CDN", _CDN_META_URL)]:
        try:
            print(f"【更新】检查更新: {meta_url}")
            data = _url_fetch(meta_url, timeout=10).strip().lstrip("\ufeff")
            if data.startswith("{"):
                return json.loads(data)
        except Exception as e:
            print(f"【更新】{label} 获取失败: {e}，尝试备用源...")
    print("【更新】所有源均失败，无法获取远程版本。")
    return {}


def download_script(url: str) -> str:
    if not url:
        return ""
    cdn_url = url.replace(_GH_RAW, _CDN_RAW) if _GH_RAW in url else ""
    for label, dl_url in [("GitHub", url), ("CDN", cdn_url)]:
        if not dl_url:
            continue
        try:
            return _url_fetch(dl_url, timeout=30)
        except Exception as e:
            print(f"【更新】{label} 下载失败: {e}，尝试备用源...")
    print("【更新】所有源均下载失败。")
    return ""


def update_single_script(name: str, local_path: str, remote_version: str, download_url: str) -> bool:
    local_version = read_local_version(local_path)
    if not remote_version:
        return False
    if parse_version(remote_version) <= parse_version(local_version):
        print(f"【更新】{name} 已是最新: {local_version}")
        return False

    print(f"【更新】{name} 发现新版本: {remote_version}（本地: {local_version}），下载中...")
    new_code = download_script(download_url)
    if not new_code:
        print(f"【更新】{name} 下载失败")
        return False

    try:
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                old = f.read()
            with open(local_path + ".bak", "w", encoding="utf-8") as f:
                f.write(old)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(new_code)
        print(f"【更新】{name} 更新成功 → {remote_version}")
        return True
    except Exception as e:
        print(f"【更新】{name} 写入失败: {e}")
        return False


def _restart_self():
    """替换完 runner 后重启自身"""
    print("【更新】linera_runner 已更新，正在自动重启...")
    time.sleep(1)
    python = sys.executable
    script = os.path.abspath(__file__)
    if getattr(sys, 'frozen', False):
        os.execv(sys.executable, [sys.executable] + sys.argv)
    else:
        os.execv(python, [python, script] + sys.argv[1:])


def try_auto_update():
    global LAST_TASK_VERSION, LAST_BASE_VERSION, LAST_RUNNER_VERSION
    global LAST_REMOTE_TASK_VERSION, LAST_REMOTE_BASE_VERSION, LAST_REMOTE_RUNNER_VERSION
    global LAST_UPDATE_STATUS

    if not CHECK_UPDATE_ON_START:
        print("【更新】自动更新已关闭。")
        LAST_UPDATE_STATUS = "disabled"
        return
    if not UPDATE_META_URL:
        print("【更新】未配置 UPDATE_META_URL，跳过自动更新。")
        LAST_UPDATE_STATUS = "no_config"
        return

    remote = fetch_remote_versions()
    if not remote:
        LAST_UPDATE_STATUS = "remote_unavailable"
        return
    print(f"【更新】远程版本: {remote}")

    base_dir = get_base_dir()
    task_path = os.path.join(base_dir, "linera_task.py")
    base_path = os.path.join(base_dir, "base_module.py")

    LAST_REMOTE_TASK_VERSION = remote.get("task_version", "")
    LAST_REMOTE_BASE_VERSION = remote.get("base_version", "")
    LAST_REMOTE_RUNNER_VERSION = remote.get("runner_version", "")

    updated = False
    if UPDATE_TASK_URL and LAST_REMOTE_TASK_VERSION:
        if update_single_script("linera_task", task_path, LAST_REMOTE_TASK_VERSION, UPDATE_TASK_URL):
            updated = True
    if UPDATE_BASE_URL and LAST_REMOTE_BASE_VERSION:
        if update_single_script("base_module", base_path, LAST_REMOTE_BASE_VERSION, UPDATE_BASE_URL):
            updated = True

    LAST_TASK_VERSION = read_local_version(task_path)
    LAST_BASE_VERSION = read_local_version(base_path)
    LAST_UPDATE_STATUS = "updated" if updated else "up_to_date"

    # runner 自更新（仅非 frozen 模式，exe 包内的 runner 无法热替换）
    if getattr(sys, 'frozen', False):
        print(f"【更新】linera_runner (exe 模式) 跳过自更新: {__version__}")
    elif UPDATE_RUNNER_URL and LAST_REMOTE_RUNNER_VERSION:
        runner_path = os.path.abspath(__file__)
        local_runner_ver = __version__
        if parse_version(LAST_REMOTE_RUNNER_VERSION) > parse_version(local_runner_ver):
            print(f"【更新】linera_runner 发现新版本: {LAST_REMOTE_RUNNER_VERSION}（本地: {local_runner_ver}），下载中...")
            new_code = download_script(UPDATE_RUNNER_URL)
            if new_code:
                try:
                    with open(runner_path, "r", encoding="utf-8") as f:
                        old = f.read()
                    with open(runner_path + ".bak", "w", encoding="utf-8") as f:
                        f.write(old)
                    with open(runner_path, "w", encoding="utf-8") as f:
                        f.write(new_code)
                    print(f"【更新】linera_runner 更新成功 → {LAST_REMOTE_RUNNER_VERSION}")
                    _restart_self()
                except Exception as e:
                    print(f"【更新】linera_runner 写入失败: {e}")
            else:
                print("【更新】linera_runner 下载失败")
        else:
            print(f"【更新】linera_runner 已是最新: {local_runner_ver}")
    LAST_RUNNER_VERSION = __version__


# ═══════════════════════════════════════════════
#  动态加载核心模块
# ═══════════════════════════════════════════════

def _load_module_from_file(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def load_core_modules():
    """
    加载 base_module.py 和 linera_task.py。
    优先从 exe 同级目录加载外部文件；失败则回退到内置版本。
    返回 (base_mod, task_mod) 或 (None, None)。
    """
    base_dir = get_base_dir()
    base_path = os.path.join(base_dir, "base_module.py")
    task_path = os.path.join(base_dir, "linera_task.py")

    base_mod = None
    task_mod = None

    # 加载 base_module
    if os.path.exists(base_path):
        print(f"【热更新】加载外部 base_module: {base_path}")
        try:
            base_mod = _load_module_from_file("base_module", base_path)
        except Exception as e:
            print(f"【热更新】加载 base_module 失败: {e}")

    if base_mod is None:
        print("【系统】回退到内置 base_module")
        try:
            import base_module as base_mod
        except ImportError as e:
            print(f"【错误】无法加载 base_module: {e}")
            return None, None

    # 加载 linera_task (依赖 base_module 已在 sys.modules)
    if os.path.exists(task_path):
        print(f"【热更新】加载外部 linera_task: {task_path}")
        try:
            task_mod = _load_module_from_file("linera_task", task_path)
        except Exception as e:
            print(f"【热更新】加载 linera_task 失败: {e}")

    if task_mod is None:
        print("【系统】回退到内置 linera_task")
        try:
            import linera_task as task_mod
        except ImportError as e:
            print(f"【错误】无法加载 linera_task: {e}")
            return base_mod, None

    return base_mod, task_mod


# ═══════════════════════════════════════════════
#  启动时初始化
# ═══════════════════════════════════════════════

try_auto_update()
base_module, task_module = load_core_modules()

_task_ver = getattr(task_module, '__version__', '?') if task_module else '未加载'
_base_ver = getattr(base_module, '__version__', '?') if base_module else '未加载'
print(f"【版本】linera_task: {_task_ver} | base_module: {_base_ver} | runner: {__version__}")

task_thread = None
is_task_running = False


def log_emitter(msg):
    socketio.emit('new_log', msg)


if base_module:
    base_module.set_logger_callback(log_emitter)

# ═══════════════════════════════════════════════
#  任务执行逻辑（asyncio 在独立线程中运行）
# ═══════════════════════════════════════════════

def run_batch_logic(thread_count, screenshot_mode=False):
    global is_task_running, base_module, task_module

    # 每次运行前重新加载，实现"热"更新
    base_module, task_module = load_core_modules()
    if not base_module or not task_module:
        log_emitter("【错误】无法加载核心模块！")
        is_task_running = False
        socketio.emit('status_update', {'running': False})
        return

    base_module.set_logger_callback(log_emitter)
    base_module.STOP_FLAG = False

    if hasattr(task_module, 'SCREENSHOT_ON_FAILURE'):
        task_module.SCREENSHOT_ON_FAILURE = screenshot_mode
        if screenshot_mode:
            log_emitter("【截图】失败截图模式已开启")

     # 显示版本号
    tv = getattr(task_module, '__version__', '?')
    bv = getattr(base_module, '__version__', '?')
    log_emitter(f"【版本】linera_task: {tv} | base_module: {bv}")

    # 加载账号
    excel_path = os.path.join(get_base_dir(), "hubshuju.xlsx")
    log_emitter(f"正在加载账号: {excel_path}")
    accounts = base_module.load_accounts(excel_path)

    if not accounts:
        log_emitter("【错误】未找到账号，请检查 hubshuju.xlsx")
        is_task_running = False
        socketio.emit('status_update', {'running': False})
        return

    log_emitter(f"共加载 {len(accounts)} 个账号，并发数: {thread_count}")

    try:
        asyncio.run(base_module.run_batch(
            accounts,
            task_module.linera_task,
            max_workers=thread_count,
        ))
    except Exception as e:
        log_emitter(f"任务执行异常: {e}")
    finally:
        is_task_running = False
        socketio.emit('status_update', {'running': False})
        log_emitter("所有任务已结束或被停止。")


# ═══════════════════════════════════════════════
#  Flask 路由 + SocketIO 事件
# ═══════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')


@socketio.on('start_task')
def handle_start_task(data):
    global task_thread, is_task_running
    if is_task_running:
        emit('new_log', "任务已经在运行中...")
        return

    try:
        threads = int(data.get('threads', 1))
    except (ValueError, TypeError, AttributeError):
        threads = 1

    screenshot_mode = bool(data.get('screenshot', False))

    is_task_running = True
    emit('status_update', {'running': True})

    task_thread = threading.Thread(
        target=run_batch_logic, args=(threads, screenshot_mode), daemon=True,
    )
    task_thread.start()


@socketio.on('connect')
def handle_connect():
    emit('version_info', {
        'task_local': LAST_TASK_VERSION,
        'base_local': LAST_BASE_VERSION,
        'runner_local': LAST_RUNNER_VERSION,
        'task_remote': LAST_REMOTE_TASK_VERSION,
        'base_remote': LAST_REMOTE_BASE_VERSION,
        'runner_remote': LAST_REMOTE_RUNNER_VERSION,
        'status': LAST_UPDATE_STATUS,
    })


@socketio.on('stop_task')
def handle_stop_task():
    global is_task_running
    if not is_task_running:
        return
    emit('new_log', "正在发送停止信号...")
    if base_module:
        base_module.stop_all_tasks()


@socketio.on('shutdown_server')
def handle_shutdown_server():
    emit('new_log', "正在关闭程序...")
    def kill():
        time.sleep(1)
        os._exit(0)
    threading.Thread(target=kill, daemon=True).start()


# ═══════════════════════════════════════════════
#  任务状态 API + 实时推送
# ═══════════════════════════════════════════════

@app.route('/api/tasks')
def api_tasks():
    if task_module and hasattr(task_module, 'TASK_STATUS'):
        return jsonify(list(task_module.TASK_STATUS.values()))
    return jsonify([])


def _get_runner_name():
    """获取本机 Runner 名称：优先环境变量，其次计算机名"""
    if RUNNER_NAME:
        return RUNNER_NAME
    import socket
    return socket.gethostname()


def _task_status_pusher():
    """后台线程：每 2 秒向前端推送 + 上报到 tasks_manager（含历史数据）"""
    while True:
        socketio.sleep(2)
        if task_module and hasattr(task_module, 'TASK_STATUS') and task_module.TASK_STATUS:
            data = list(task_module.TASK_STATUS.values())
            socketio.emit('task_status_update', data)

            if REPORT_URL:
                try:
                    payload = json.dumps({
                        'runner': _get_runner_name(),
                        'tasks': data,
                    }).encode('utf-8')
                    req = urllib.request.Request(
                        REPORT_URL, data=payload,
                        headers={'Content-Type': 'application/json'},
                        method='POST',
                    )
                    urllib.request.urlopen(req, timeout=3)
                except Exception:
                    pass


socketio.start_background_task(_task_status_pusher)


# ═══════════════════════════════════════════════
#  入口
# ═══════════════════════════════════════════════

if __name__ == '__main__':
    port = 5001
    print("=" * 50)
    print("  Linera Prediction Market 控制台")
    print(f"  请在浏览器访问: http://127.0.0.1:{port}")
    print("=" * 50)

    def open_browser():
        time.sleep(1.5)
        import webbrowser
        webbrowser.open(f"http://127.0.0.1:{port}")
    threading.Thread(target=open_browser, daemon=True).start()

    socketio.run(app, host="0.0.0.0", debug=False, port=port, allow_unsafe_werkzeug=True)
