from flask import Flask

import app as backend


def test_api_security_headers():
    response = backend.app.test_client().get("/api/missing-resource")
    assert response.status_code == 404
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "SAMEORIGIN"


def test_request_size_limit():
    response = backend.app.test_client().post(
        "/api/tickets", data=b"x" * (1024 * 1024 + 1),
        content_type="application/json", headers={"Authorization": "Bearer demo-admin"},
    )
    assert response.status_code == 413
    assert response.get_json()["ok"] is False


def test_api_errors_preserve_http_headers_and_hide_internal_details():
    from utils.request_policy import configure_http

    app = Flask(__name__)
    configure_http(app)

    @app.get("/api/check")
    def check():
        raise RuntimeError("private detail")

    client = app.test_client()
    failure = client.get("/api/check")
    assert failure.status_code == 500
    assert failure.get_json()["ok"] is False
    assert b"private detail" not in failure.data

    wrong_method = client.post("/api/check")
    assert wrong_method.status_code == 405
    assert wrong_method.get_json()["ok"] is False
    assert "GET" in wrong_method.headers["Allow"]
