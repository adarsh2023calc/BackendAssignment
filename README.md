# BackendAssignment




from pathlib import Path

readme_content = """# 🧠 queuectl — CLI-based Background Job Queue System

`queuectl` is a lightweight, production-style **background job queue system** built in **Python**.  
It supports enqueuing shell commands as jobs, processing them with worker processes, retrying failures using **exponential backoff**, and maintaining a **Dead Letter Queue (DLQ)** for permanently failed jobs.

---

## ✨ Features

- 🧾 **Job management** — enqueue, list, and inspect jobs  
- ⚙️ **Multiple workers** — process jobs concurrently  
- 🔁 **Automatic retries** — exponential backoff on failure  
- 💀 **Dead Letter Queue (DLQ)** — permanently failed jobs are stored safely  
- 💾 **Persistent storage** — jobs stored in SQLite (`queue.db`)  
- 🧰 **CLI interface** — fully controllable from the terminal  

---

## 📦 Installation

```bash
git clone https://github.com/yourusername/queuectl.git
cd queuectl
chmod +x queuectl
sudo ln -s $(pwd)/queuectl /usr/local/bin/queuectl   # optional global usage
```


---

##  Job Format

```bash
{
  "id": "job-1",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z"
}
```

---

## 🚀 Usage
1️⃣ Enqueue a Job
```bash 
queuectl enqueue --json '{"id":"job1","command":"echo Hello","max_retries":2}'
```

or 
 ```bash
queuectl enqueue --command "ls-l"
```