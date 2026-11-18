"""Shared HTTP helpers for the Flask API.

Kept dependency-light so route modules do not need to import the application
bootstrap module just to build a standard response or parse JSON.
"""

from flask import jsonify, request


def ok(data):
    return jsonify({"ok": True, "data": data})


def err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


def json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return None, err("请求体必须是 JSON 对象")
    return body, None


def positive_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and value.is_integer():
        number = int(value)
    elif isinstance(value, str) and value.strip().isdecimal():
        try:
            number = int(value.strip())
        except ValueError:
            return None
    else:
        return None
    return number if number > 0 else None
