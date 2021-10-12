import os
import json
import time
import socket
import requests
from collections import OrderedDict
import psutil
import subprocess
import sys
import torch

import cypy
from cypy.misc_utils import warning_prompt, get_cmd_output
from cypy.cli_utils import simple_cli

def get_occupy_gpu_script_path():
    return os.path.join(cypy.__path__[0], 'taiji', 'occupy_gpu_script.py')

def post_to_robot(content):
    ROBOT_KEY = os.getenv('ROBOT_KEY')
    if ROBOT_KEY is None:
        raise ValueError('ERR in func `post_to_robot`: ROBOT_KEY env is not set!')
    ROBOT_URL = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={ROBOT_KEY}"
    data = {
        "msgtype": "text",
        "text": {
            "content": content
        }
    }
    data = json.dumps(data, ensure_ascii=False)
    data = data.encode(encoding="utf-8")
    try:
        requests.post(url=ROBOT_URL, data=data, timeout=10)
    except requests.exceptions.ConnectionError:
        pass


def get_ip_naive():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


def check_get_env(env_name):
    allow_names = ['NODE_LIST', 'NODE_IP_LIST', 'HOST_NUM', 'HOST_GPU_NUM', 'NODE_NUM', 'CHIEF_IP', 'LOCAL_IP', 'JIZHI_WORKSPACE_PATH', 'TJ_TASK_ID', 'TJ_INSTANCE_ID', 'GPU_NAME', 'RTX_NAME', 'INDEX', 'LOCAL_HOSTNAME', 'TAIJI_GPU_LOCATION', 'ELASTIC', 'TAIJI_META_DIR']
    assert env_name in allow_names, f'\n{env_name} is not allowed. \nallow_names are {allow_names}. '
    
    env_val = os.getenv(env_name)
    assert env_val is not None, f'\n{env_name} is not set. '

    return env_val


def get_taiji_meta_dir():
    taiji_meta_dir = check_get_env('TAIJI_META_DIR')
    assert os.path.exists(taiji_meta_dir), f'base_dir {taiji_meta_dir} not exists!'
    return taiji_meta_dir


def get_lock_path():
    taiji_meta_dir = get_taiji_meta_dir()
    lock_path = os.path.join(taiji_meta_dir, 'lock')
    return lock_path


def get_info_path():
    taiji_meta_dir = get_taiji_meta_dir()
    info_path = os.path.join(taiji_meta_dir, 'info.json')
    return info_path


def save_info_on_start():
    # only save on launcher node
    is_launcher = 'launcher' in check_get_env('LOCAL_HOSTNAME')
    if not is_launcher:
        return

    host_ip = check_get_env('CHIEF_IP')
    task_id = check_get_env('TJ_TASK_ID')
    instance_id = check_get_env('TJ_INSTANCE_ID')
    is_ela = 'Y' if check_get_env('ELASTIC').upper() in ['TRUE', 'ON', '1', 'Y', 'YES'] else 'N'

    lock_file = get_lock_path()
    if not os.path.exists(lock_file):
        os.system(f'echo "free" > {lock_file}')

    info_file = get_info_path()
    if not os.path.exists(info_file):
        json.dump({}, open(info_file, 'w'))

    cnt = 0
    while True:
        lock_status = open(lock_file, 'r').read().strip()
        assert lock_status in ['locked', 'free']
        if lock_status == 'locked' and cnt < 30:
            print('Other proc is writing lock_file, retry in 1 second...')
            cnt += 1
            time.sleep(1)
        elif lock_status == 'locked':
            print('save task info hangs!')
            post_to_robot(f'[❌启动时服务器信息写入死锁]\nIP: {host_ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}')
            raise
        else:
            break
    
    try:
        os.system(f'echo "locked" > {lock_file}')

        append_info = {
            host_ip: {
                'task_id': task_id,
                'instance_id': instance_id,
                'commit_time': time.time(),
                'is_ela': is_ela
            } 
        }

        all_info = json.load(open(info_file, 'r', encoding='utf-8'))
        all_info.update(append_info)
        json.dump(all_info, open(info_file, 'w'), ensure_ascii=False, indent=4, sort_keys=True)
    except Exception as e:
        print(f'Err happened in save_info. Err Msg: {str(e)}.')
        post_to_robot(f'[❌启动时服务器信息JSON写入失败]\nIP: {host_ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}\nError: {str(e)}')
    finally:
        os.system(f'echo "free" > {lock_file}')

    if os.getenv("START_NOTICE") is not None and os.getenv("START_NOTICE").upper() in ['TRUE', 'ON', '1', 'Y', 'YES']:
        post_to_robot(f'[✅服务器启动成功]\nIP: {host_ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}\n')


def report_job_done():
    # only save on launcher node
    is_launcher = 'launcher' in check_get_env('LOCAL_HOSTNAME')
    if not is_launcher:
        return

    host_ip = check_get_env('CHIEF_IP')
    task_id = check_get_env('TJ_TASK_ID')
    instance_id = check_get_env('TJ_INSTANCE_ID')
    is_ela = 'Y' if check_get_env('ELASTIC').upper() in ['TRUE', 'ON', '1', 'Y', 'YES'] else 'N'

    lock_file = get_lock_path()
    info_file = get_info_path()

    cnt = 0
    while True:
        lock_status = open(lock_file, 'r').read().strip()
        assert lock_status in ['locked', 'free']
        if lock_status == 'locked' and cnt < 30:
            print('Other proc is writing lock_file, retry in 1 second...')
            cnt += 1
            time.sleep(1)
        elif lock_status == 'locked':
            print('save task info hangs!')
            post_to_robot(f'[❌结束时服务器信息JSON写入失败]\nIP: {host_ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}')
            raise
        else:
            break

    try:
        os.system(f'echo "locked" > {lock_file}')

        json_info = json.load(open(info_file, 'r', encoding='utf-8'))
        if host_ip in json_info:
            json_info.pop(host_ip)
        
        json.dump(json_info, open(info_file, 'w'), ensure_ascii=False, indent=4, sort_keys=True)

    except Exception as e:
        print(f'Err happened in save_info. Err Msg: {str(e)}.')
        post_to_robot(f'[Server Info Json Dump Fail On Job Done]\nIP: {host_ip}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}\nError: {str(e)}')
    finally:
        os.system(f'echo "free" > {lock_file}')

    post_to_robot(f'[⚠️服务器结束运行]\nIP: {host_ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}\n')
    

def jump_server():
    lock_file = get_lock_path()
    info_file = get_info_path()

    def save_json(json_content):
        cnt = 0
        while True:
            lock_status = open(lock_file, 'r').read().strip()
            assert lock_status in ['locked', 'free']
            if lock_status == 'locked' and cnt < 30:
                print('Other proc is writing lock_file, retry in 1 second...')
                cnt += 1
                time.sleep(1)
            elif lock_status == 'locked':
                print(f'lock file {lock_file} is occupied for a long time. Manually check it?')
                raise
            else:
                break

        try:
            os.system(f'echo "locked" > {lock_file}')
            json.dump(json_content, open(info_file, 'w'), ensure_ascii=False, indent=4, sort_keys=True)
        except Exception as e:
            print(f'Err happened in save_json. Err Msg: {str(e)}.')
        finally:
            os.system(f'echo "free" > {lock_file}')
        
    info = OrderedDict(json.load(open(info_file, 'r')))
    info = OrderedDict(sorted(info.items(), key=lambda x: x[1]['commit_time']))

    print('>>> SERVER INFO <<<')
    print('-------' * 18)
    print('{:<6s} {:<3s} {:<15s} {:<35s} {:<35s} {:<20s} {}'.format('INDEX', 'ELA', 'SERVER_IP', 'TASK_ID', 'INST_ID', 'COMMIT_TIME', 'REMARK'))
    print('-------' * 18)
    list_info = []
    idx = 0
    for ip, v in info.items():
        commit_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v['commit_time']))
        inst_id = v['instance_id']
        task_id = v['task_id']
        is_ela = v['is_ela']
        remark = '' if not 'remark' in v else v['remark']
        line = f'{idx:^6d} {is_ela:^3s} {ip:<15s} {task_id:<35s} {inst_id:<35s} {commit_time:<20s} {remark:<}'
        print(line)
        list_info.append([ip, is_ela, task_id, inst_id, commit_time, remark])
        idx += 1

    print()
    print('Avalable cmds: idx (exec connect) || s + idx (ssh connect) || d + idx (delete item) || r + idx (modify remark) || l + idx (get inst url)')
    prompt = input('Jump to: ')
    assert prompt.upper() == 'Q' or prompt.isdigit() or (prompt.startswith('s') and prompt[1:].isdigit()) or (prompt.startswith('d') and prompt[1:].isdigit()) or (prompt.startswith('r') and prompt[1:].isdigit()) or (prompt.startswith('l') and prompt[1:].isdigit())
    if prompt.upper() == 'Q':
        return
    elif prompt.isdigit():
        ip, is_ela, task_id, inst_id, commit_time, remark = list_info[int(prompt)]
        cmd = f'jizhi_client exec {task_id} {inst_id} bash'
        os.system(cmd)
    elif prompt.startswith('s'):
        ip, is_ela, task_id, inst_id, commit_time, remark = list_info[int(prompt[1:])]
        cmd = f'ssh root@{ip}'
        print(f'ssh to {ip}...')
        os.system(cmd)
    elif prompt.startswith('d'):
        inp_idx = int(prompt[1:])
        ip, is_ela, task_id, inst_id, commit_time, remark = list_info[inp_idx]
        warning_prompt(f'Del: `{inp_idx} {is_ela} {ip} {task_id} {inst_id} {commit_time} {remark}`?')
        info.pop(ip)
        save_json(info)
    elif prompt.startswith('r'):
        inp_idx = int(prompt[1:])
        ip, is_ela, task_id, inst_id, commit_time, remark = list_info[inp_idx]
        inp_remark = input(f'Remark for: `{inp_idx} {is_ela} {ip} {task_id} {inst_id} {commit_time} {remark}`:\n')
        info[ip]['remark'] = inp_remark.strip()
        save_json(info)
    elif prompt.startswith('l'):
        ip, is_ela, task_id, inst_id, commit_time, remark = list_info[int(prompt[1:])]
        inst_url = f'http://taiji.oa.com/#/project-list/jizhi/task-inst-detail/{inst_id}'
        print(f'Inst url: {inst_url}')

### taiji occupy gpu ###
# (1) launch_occupy_gpu: used in taiji start.sh entry. 'python3 -c "from cypy.taiji import launch_occupy_gpu;launch_occupy_gpu()"', will occupy cpu immediately in front and launch an auto occupy monitor daemon at the same time.
#         so if you see `occupy_gpu_when_idle_loop` in ps command, it means a monitor daemon is running.
# (2) occupy_gpu_when_idle_loop: helper func for monitor gpu and occupying while gpu is idle.
#         if you want to add a daemon in normal func, you can add the following code before the main logic:
#             nohup python3 -c "from cypy.taiji import occupy_gpu_when_idle_loop;occupy_gpu_when_idle_loop() >/dev/null 2>&1 &"

def launch_occupy_gpu(gpu_num=0, gpu_level=1.0, keep_in_front=True, auto_occupy_gpu=True, occupy_scan_interval=60*20):
    args = simple_cli(
        gpu_num=gpu_num,
        gpu_level=gpu_level,
        keep_in_front=keep_in_front,
        auto_occupy_gpu=auto_occupy_gpu,
        occupy_scan_interval=occupy_scan_interval
    )

    if args.gpu_num == 0:
        args.gpu_num = torch.cuda.device_count()

    if args.auto_occupy_gpu:
        cmd = f'nohup python3 -c "from cypy.taiji import occupy_gpu_when_idle_loop;occupy_gpu_when_idle_loop({args.gpu_num}, {args.gpu_level}, {args.occupy_scan_interval}, 0)" >/dev/null 2>&1 &'
        os.system(cmd)

    for i in range(args.gpu_num):
        cmd = f'CUDA_VISIBLE_DEVICES={i} nohup python3 {get_occupy_gpu_script_path()} --gpu_level {args.gpu_level} >/dev/null 2>&1 &'
        os.system(cmd)  
    
    if args.keep_in_front:
        while True:
            time.sleep(100)
    

def occupy_gpu_when_idle_loop(occupy_gpu_num=0, gpu_level=1.0, interval=60*20, sleep_time=0):
    assert interval >= 5 and interval % 5 == 0
    args = simple_cli(
        occupy_gpu_num=occupy_gpu_num,
        gpu_level=gpu_level,
        interval=interval,
        sleep_time=sleep_time
    )

    time.sleep(args.sleep_time)

    # TODO: make port configurable
    dist_train_ports = [10010, 12345, 29500, 10086]
    def get_train_pids():
        ret_pids = []
        for port in dist_train_ports:
            cmd = "lsof -i:%s | tail -n +2 | awk '{print $2}'" % port
            res = get_cmd_output(cmd)[0].strip()
            if res:
                ret_pids.extend(res.strip().split('\n'))
        return ret_pids

    def get_occupy_pids():
        cmd = "ps aux | grep occupy_gpu_script.py | grep -v grep | awk '{print $2}'"
        res = get_cmd_output(cmd)[0].strip()
        if not res:
            return []
        return res.strip().split('\n')

    def kill_pids(pids):
        for pid in pids:
            pid = int(pid)
            if psutil.pid_exists(pid):
                p = psutil.Process(pid)
                p.kill()

    def launch_occupy():
        # cmd = f'nohup python3 -c "from cypy.taiji import launch_occupy_gpu;launch_occupy_gpu({args.occupy_gpu_num} {args.gpu_level} False False {args.interval})" >/dev/null 2>&1 &'
        # os.system(cmd)
        for i in range(args.occupy_gpu_num):
            cmd = f'CUDA_VISIBLE_DEVICES={i} nohup python3 {get_occupy_gpu_script_path()} --gpu_level {args.gpu_level} >/dev/null 2>&1 &'
            os.system(cmd) 
    
    while True:
        for i in range(args.interval, 0, -1):
            sys.stdout.write(f'Occupy GPU next schedule in {i} s...\r')
            sys.stdout.flush()
            time.sleep(1)

            if i % 5 == 0:
                occupy_pids = get_occupy_pids()
                train_pids = get_train_pids()
                if train_pids and occupy_pids:
                    sys.stdout.write('\n')
                    print(f'Occupy pauses as train pids found!')
                    kill_pids(occupy_pids)

                    json_info = json.load(open(get_info_path(), 'r', encoding='utf-8'))
                    ip = get_ip_naive()
                    if ip in json_info:
                        task_id, instance_id, is_ela = json_info[ip]['task_id'], json_info[ip]['instance_id'], json_info[ip]['is_ela']
                        post_to_robot(f'[👉占卡暂停]\nIP: {ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}\n')
                    else:
                        # TODO: CHIEF_IP, LOCAL_IP to be decided
                        post_to_robot(f'[👉占卡暂停]\nIP: {ip}\nMay be a slave node.')

        occupy_pids = get_occupy_pids()
        train_pids = get_train_pids()
        if (not train_pids) and (not occupy_pids):
            sys.stdout.write('\n')
            print(f'Occupy starts!')
            launch_occupy()

            json_info = json.load(open(get_info_path(), 'r', encoding='utf-8'))
            ip = get_ip_naive()
            if ip in json_info:
                task_id, instance_id, is_ela = json_info[ip]['task_id'], json_info[ip]['instance_id'], json_info[ip]['is_ela']
                post_to_robot(f'[👉启动占卡]\nIP: {ip}\nELA: {is_ela}\nTASK_ID: {task_id}\nINSTANCE_ID: {instance_id}\n')
            else:
                post_to_robot(f'[👉启动占卡]\nIP: {ip}\nMay be a slave node.')
        else:
            sys.stdout.write('\n')
            print('Nothing to be changed.')