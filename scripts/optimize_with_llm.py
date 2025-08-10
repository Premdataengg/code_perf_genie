import os, sys, json, datetime, subprocess
from pathlib import Path
from typing import List, Dict

INCLUDE_GLOBS = ["main.py", "common.py", "src/**/*.py"]
EXCLUDE_DIRS = {".git", ".github", ".venv", "venv", "node_modules", "build", "dist", "__pycache__", "logs", "data/generated"}
MAX_FILES = 10
LOOKBACK_HOURS = 36
MODEL = "gpt-4.1"
PROMPT_PATH = Path("scripts/prompt_template.txt")
PR_BODY_PATH = Path("llm_pr_body.md")

def run(cmd: List[str], check=True) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return res.stdout.strip()

def changed_files_within(hours: int) -> List[Path]:
    since = (datetime.datetime.utcnow() - datetime.timedelta(hours=hours)).isoformat() + "Z"
    out = run(["git", "log", f'--since="{since}"', "--name-only", "--pretty=format:"], check=False)
    files = [Path(p.strip()) for p in out.splitlines() if p.strip()]
    seen = set(); out_paths = []
    for p in files:
        if p.exists() and str(p) not in seen:
            seen.add(str(p)); out_paths.append(p)
    return out_paths

def ok_path(p: Path) -> bool:
    parts = set(p.parts)
    if any(d in parts for d in EXCLUDE_DIRS): return False
    return p.is_file()

def collect_files() -> List[Path]:
    files: List[Path] = []
    recent = changed_files_within(LOOKBACK_HOURS)
    globs = []
    for g in INCLUDE_GLOBS: globs.extend(Path(".").glob(g))

    def add(p):
        if ok_path(p) and p not in files: files.append(p)

    for p in recent:
        for g in INCLUDE_GLOBS:
            if p.match(g):
                add(p)
                if len(files) >= MAX_FILES: return files

    if len(files) < MAX_FILES:
        for p in globs:
            add(p)
            if len(files) >= MAX_FILES: break
    return files

def load_payload(paths: List[Path]) -> str:
    chunks = []
    for p in paths:
        try:
            txt = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if len(txt) > 160_000:  # guardrail
            continue
        chunks.append(f"---FILE START---\nPATH: {p.as_posix()}\nCONTENT:\n{txt}\n---FILE END---")
    return "\n\n".join(chunks)

def call_openai(input_text: str) -> Dict:
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key: raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)
    sys_prompt = PROMPT_PATH.read_text(encoding="utf-8")
    completion = client.chat.completions.create(
        model=MODEL, temperature=0.2,
        messages=[{"role": "system", "content": sys_prompt},
                  {"role": "user", "content": input_text}],
        response_format={"type": "json_object"},
    )
    return json.loads(completion.choices[0].message.content)

def write_changes(changes: List[Dict]) -> List[Path]:
    allowed = set(p.as_posix() for g in INCLUDE_GLOBS for p in Path(".").glob(g))
    touched = []
    for ch in changes:
        path = Path(ch["path"]).as_posix()
        if path not in allowed:  # safety
            continue
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(ch["new_content"], encoding="utf-8")
        touched.append(Path(path))
    return touched

def main():
    files = collect_files()
    if not files: 
        print("No candidate files found."); return
    payload = load_payload(files)
    if not payload:
        print("No readable files."); return

    print(f"Reviewing {len(files)} files via LLM...")
    data = call_openai(payload)
    changes = data.get("changes") or []
    summary = data.get("summary") or ""
    notes = data.get("notes") or []

    if not changes:
        print("LLM suggested no edits today."); return

    touched = write_changes(changes)
    if not touched:
        print("No valid files updated after safety filters."); return

    bullets = "\n".join([f"- {n}" for n in notes]) if notes else ""
    pr_body = f"""## Daily Spark Optimization

**Summary**
{summary}

**Files updated**
{''.join([f"- `{p.as_posix()}`\\n" for p in touched])}

**Notes**
{bullets}

> This PR was generated automatically. Please run tests and review carefully.
"""
    PR_BODY_PATH.write_text(pr_body, encoding="utf-8")
    print(f"Prepared PR body with {len(touched)} file(s).")

if __name__ == "__main__":
    main()
