"""HTTP hooks shared by the dashboard and API."""
import os
from time import perf_counter

from flask import g, request
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge


def configure_http(app):
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024

    @app.before_request
    def check_content_length():
        if request.content_length is not None and request.content_length > app.config["MAX_CONTENT_LENGTH"]:
            raise RequestEntityTooLarge()

    @app.after_request
    def security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        response = error.get_response()
        if request.path.startswith("/api/"):
            message = "服务器内部错误" if error.code == 500 else error.name
            response.set_data(app.json.dumps({"ok": False, "error": message}))
            response.mimetype = "application/json"
        return response

    def _timing_header_enabled() -> bool:
        return os.getenv("API_TIMING_HEADER", "").strip().lower() in ("1", "true", "yes", "on")


    @app.before_request
    def _request_timer():
        if _timing_header_enabled():
            g.request_started = perf_counter()


    @app.after_request
    def _request_timing(resp):
        started = getattr(g, "request_started", None)
        if started is not None:
            resp.headers["X-Response-Time-Ms"] = f"{(perf_counter() - started) * 1000:.2f}"
        return resp


    def _cors_allowed_origins() -> set[str]:
        raw = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:5096,http://127.0.0.1:5096")
        return {origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()}


    @app.after_request
    def _cors(resp):
        origin = request.headers.get("Origin")
        allowed = _cors_allowed_origins()
        if origin and (origin in allowed or "*" in allowed):
            resp.headers["Access-Control-Allow-Origin"] = "*" if "*" in allowed else origin
            resp.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
            resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,PUT,DELETE,OPTIONS"
            resp.headers["Access-Control-Max-Age"] = "600"
            if "*" not in allowed:
                resp.vary.add("Origin")
        return resp
