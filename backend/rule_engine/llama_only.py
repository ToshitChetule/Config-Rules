# backend/rule_engine/llama_only.py
import requests
import json
import time
from typing import List, Dict, Any, Set

OLLAMA_URL = "http://localhost:11434/api/chat"
LLAMA_MODEL_NAME = "llama3"  # your local Ollama model name


def chunk_list(lst: List[Any], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def send_prompt_to_ollama(prompt: str, timeout: int = 60):
    payload = {
        "model": LLAMA_MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }
    r = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_json_array_from_text(text: str):
    """
    Extract the first JSON array ([ ... ]) from model output.
    """
    try:
        start = text.index("[")
        end = text.rindex("]") + 1
        json_block = text[start:end]
        return json.loads(json_block)
    except Exception as e:
        raise ValueError(f"Could not extract JSON array. Raw: {text}") from e


def build_prompt_for_chunk(chunk_records: List[Dict[str, Any]]):
    sample = json.dumps(chunk_records, indent=2, ensure_ascii=False)

    return f"""
You are a configuration rule-mining engine.

From these configuration rows, infer all deterministic IF–THEN rules.

FORMAT for each rule:
"IF <Column> = <Value> THEN <Column> = <Value>"

STRICT REQUIREMENTS:
- Output ONLY a JSON array of rule strings.
- No explanation text.
- No markdown.
- No extra notes.
- No text outside the JSON array.

DATA:
{sample}

Return ONLY the JSON array.
"""


def infer_rules_from_records(records: List[Dict[str, Any]], chunk_size: int = 30, max_retries: int = 2, verbose: bool = False) -> List[str]:
    all_rules_set: Set[str] = set()

    if not records:
        return []

    chunks = list(chunk_list(records, chunk_size))

    if verbose:
        print(f"🔍 Total rows: {len(records)} → sending {len(chunks)} chunks")

    for i, chunk in enumerate(chunks, start=1):

        prompt = build_prompt_for_chunk(chunk)
        attempt = 0
        rules_list = []

        while attempt <= max_retries:
            try:
                if verbose:
                    print(f"⏳ Sending chunk {i}/{len(chunks)} → attempt {attempt+1}")

                resp = send_prompt_to_ollama(prompt, timeout=120)
                content = resp.get("message", {}).get("content", "").strip()

                rules_list = extract_json_array_from_text(content)

                if verbose:
                    print(f"📤 Raw rules from chunk {i}: {rules_list}")

                for r in rules_list:
                    if isinstance(r, str):
                        clean = " ".join(r.split()).strip()
                        if clean.startswith("IF ") and " THEN " in clean:
                            all_rules_set.add(clean)

                break  # success → exit retry loop

            except Exception as e:
                attempt += 1
                print(f"⚠️ Chunk {i} attempt {attempt} failed: {e}")

                if attempt > max_retries:
                    print(f"❌ Giving up on chunk {i}")
                else:
                    time.sleep(1.5 * attempt)

        if verbose:
            print(f"✅ Chunk {i} processed → {len(rules_list)} raw rules")

    return sorted(all_rules_set)
