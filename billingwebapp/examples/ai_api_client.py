"""Dependency-free reference client for the separate Call Assistant.

Use environment variables; never put production credentials in this file.
This helper demonstrates transport only and contains no voice/AI logic.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid


class ClinicAIAPIError(RuntimeError):
    def __init__(self, status, code, message, request_id=None):
        super().__init__("{} ({}; request_id={})".format(message, code, request_id))
        self.status = status
        self.code = code
        self.request_id = request_id


class ClinicAIClient:
    def __init__(self, base_url, client_id, client_secret):
        self.base_url = base_url.rstrip("/") + "/api/v1/ai"
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def _request(self, method, path, *, body=None, headers=None, authenticated=True):
        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Request-ID": "req-{}".format(uuid.uuid4().hex),
        }
        request_headers.update(headers or {})
        if authenticated:
            if not self.access_token:
                self.authenticate()
            request_headers["Authorization"] = "Bearer {}".format(self.access_token)
        encoded = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            self.base_url + path, data=encoded, headers=request_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read().decode("utf-8"))
            error = payload.get("error") or {}
            raise ClinicAIAPIError(
                exc.code,
                error.get("code", "HTTP_ERROR"),
                error.get("message", "Clinic API request failed"),
                payload.get("request_id"),
            ) from exc
        return payload["data"]

    def authenticate(self, scopes=None):
        data = self._request(
            "POST",
            "/auth/token",
            body={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": " ".join(scopes or []),
            },
            authenticated=False,
        )
        self.access_token = data["access_token"]
        return data

    def clinic_status(self, location_id=None):
        suffix = "?location_id={}".format(int(location_id)) if location_id else ""
        return self._request("GET", "/clinic/status" + suffix)

    def doctors(self, location_id=None):
        suffix = "?location_id={}".format(int(location_id)) if location_id else ""
        return self._request("GET", "/doctors" + suffix)

    def slots(self, doctor_id, location_id, appointment_date, period=None):
        query = {"doctor_id": doctor_id, "location_id": location_id, "date": appointment_date}
        if period:
            query["period"] = period
        return self._request("GET", "/appointments/slots?" + urllib.parse.urlencode(query))

    def identify(self, mobile, name=None):
        body = {"mobile": mobile}
        if name:
            body["name"] = name
        return self._request("POST", "/patients/identify", body=body)

    def send_otp(self, identification_ref):
        return self._request(
            "POST", "/verification/otp/send", body={"identification_ref": identification_ref}
        )

    def verify_otp(self, identification_ref, otp):
        return self._request(
            "POST",
            "/verification/otp/verify",
            body={"identification_ref": identification_ref, "otp": otp},
        )

    def book(self, patient_session, doctor_id, location_id, start_at, idempotency_key):
        return self._request(
            "POST",
            "/appointments",
            body={"doctor_id": doctor_id, "location_id": location_id, "start_at": start_at},
            headers={"X-Patient-Session": patient_session, "Idempotency-Key": idempotency_key},
        )

    def reports(self, patient_session):
        return self._request("GET", "/reports/status", headers={"X-Patient-Session": patient_session})


if __name__ == "__main__":
    client = ClinicAIClient(
        os.environ["CLINIC_API_BASE_URL"],
        os.environ["CLINIC_AI_CLIENT_ID"],
        os.environ["CLINIC_AI_CLIENT_SECRET"],
    )
    print(json.dumps(client.clinic_status(), indent=2))
