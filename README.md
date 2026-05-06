# 资产存活探测 (cunhuo)

上传 txt 域名列表 → 后台多线程探活 → 下载 alive.csv / dead.csv。
带任务队列（同时跑 1 个）、提前/终止/删除、SQLite 持久化。

---

## 安装

```bash
pip install -r requirements.txt
```

---

## 三种跑法

### 1. 临时跑（开发/测试）

```bash
python app.py
# 访问 http://127.0.0.1:5000
```
关掉终端就死。

### 2. 后台跑（screen / nohup）

**screen：**
```bash
screen -S cunhuo
./start.sh
# Ctrl+A 然后 D 脱离，关 ssh 也活着
# 回来：screen -r cunhuo
```

**nohup：**
```bash
nohup ./start.sh > app.log 2>&1 &
# 看日志：tail -f app.log
# 停止：pkill -f gunicorn   或   pkill -f app.py
```

### 3. systemd（推荐，崩了自动拉起 + 开机自启）

把代码放到 `/opt/cunhuo`（或改 `cunhuo.service` 里的 `WorkingDirectory` / `ExecStart` 路径）：

```bash
sudo cp cunhuo.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cunhuo

# 查看状态/日志
systemctl status cunhuo
journalctl -u cunhuo -f
```

常用：
```bash
sudo systemctl restart cunhuo   # 重启
sudo systemctl stop cunhuo      # 停止
```

---

## 抗崩说明

不管哪种跑法，**任务数据都已经持久化**：

- 队列、任务元数据 → `jobs.db` (SQLite)
- 每个任务的输入域名、CSV 结果 → `jobs/<id>/`

**进程突然死掉的影响：**

| 状态           | 重启后                                                         |
|----------------|----------------------------------------------------------------|
| 排队中 queued  | **自动接着跑**（worker 起来后扫表）                            |
| 运行中 running | 标记为 `interrupted`，**已写入的部分 CSV 仍可下载**，不会续跑 |
| 已完成         | 不变                                                           |

也就是说：哪怕进程裸奔挂掉，你已经探活的结果都不会丢，排队里没动过的任务会接着跑。中断的那 1 个需要手动重新建。

---

## 访问鉴权 (Basic Auth)

设置两个环境变量即可启用，**任一为空都视为关闭**：

```bash
export CUNHUO_USER=admin
export CUNHUO_PASS=改成你的强密码
./start.sh
```

设了之后访问任何页面（包括 `/jobs`、`/upload`、`/download` 等所有接口）浏览器都会先弹账号密码框。验证通过后浏览器会自动缓存 credentials，整个会话不用再输。

systemd 部署在 `cunhuo.service` 里加：
```ini
Environment=CUNHUO_USER=admin
Environment=CUNHUO_PASS=改成你的强密码
```

宝塔部署去 Python 项目管理器 → 项目 → 「环境变量」加同名两个变量，重启项目生效。

⚠️ Basic Auth 是**明文传输**（base64 不算加密）。**只有上 HTTPS 才安全**，纯 HTTP 下账号密码任何人抓包都能看到。生产环境务必配证书。

---

## 重要：worker 必须为 1

`start.sh` 里写死了 `gunicorn -w 1`。**别改成多 worker**。

队列的调度线程和实时进度（`live_jobs` dict）都在 Python 进程内存里。多 worker 各自跑一份会重复执行 + 状态错乱。

如果以后真要扩并发，需要把队列搬到 Redis / DB 锁，目前不支持。

---

## 文件目录

```
cunhuo/
├── app.py              # Flask 主程序
├── cunhuo.py           # 原 CLI 脚本（仍可用 python cunhuo.py -f xx.txt）
├── start.sh            # gunicorn 启动脚本
├── cunhuo.service      # systemd 单元文件
├── requirements.txt
├── templates/
│   └── index.html
├── jobs.db             # 自动生成
└── jobs/               # 自动生成,每个任务一个子目录
    └── <job_id>/
        ├── input.txt   # 上传的域名
        ├── alive.csv
        └── dead.csv
```
