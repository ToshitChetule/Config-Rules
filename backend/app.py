# backend/app.py
import os
import json
import time
from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import pandas as pd
# from rule_engine.llama_only import infer_rules_from_records

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_DIR


@app.route("/api/upload", methods=["POST"])
def upload_file():
    file = request.files.get("file")
    if not file:
        return jsonify({"message": "No file uploaded"}), 400

    filename = secure_filename(file.filename)
    path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(path)

    # If you want to return a preview path (dev/test), include local path like below
    preview_path = "/mnt/data/5bc9ac59-af8b-4818-96e1-1bc44e873702.png"

    return jsonify({"file_id": filename, "preview": preview_path})


@app.route("/preview/<path:filename>")
def preview(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


from rule_engine.deterministic_miner import mine_simple_rules

@app.route("/api/generate-rules", methods=["POST"])
def generate_rules():
    payload = request.get_json() or {}
    file_id = payload.get("file_id")
    if not file_id:
        return jsonify({"message": "Missing file_id"}), 400

    filepath = os.path.join(app.config["UPLOAD_FOLDER"], file_id)
    if not os.path.exists(filepath):
        return jsonify({"message": "File not found"}), 404

    df = pd.read_excel(filepath).fillna("")

    rules = mine_simple_rules(df)

    print(f"✅ Simple rules generated: {len(rules)}")

    return jsonify({"rules": rules})


if __name__ == "__main__":
    app.run(port=5000, debug=True)
