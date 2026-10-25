"""Flask 薄 API 骨架（小粉接 6～10 个接口）。"""
from flask import Flask, jsonify

app = Flask(__name__)


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "service": "sz-ev-api"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
