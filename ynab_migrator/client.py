from __future__ import annotations

import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional

import requests


class YNABApiError(RuntimeError):
    def __init__(self, status_code: int, message: str, body: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body or {}


@dataclass
class RetryConfig:
    max_retries: int = 8
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 120.0


class RollingRateLimiter:
    def __init__(self, requests_per_hour: int = 200, logger: Optional[logging.Logger] = None):
        self.requests_per_hour = requests_per_hour
        self._timestamps: Deque[float] = deque()
        self.logger = logger

    def acquire(self) -> float:
        slept_seconds = 0.0
        while True:
            now = time.time()
            one_hour_ago = now - 3600
            while self._timestamps and self._timestamps[0] < one_hour_ago:
                self._timestamps.popleft()

            if len(self._timestamps) < self.requests_per_hour:
                self._timestamps.append(now)
                return slept_seconds

            sleep_for = max(1.0, self._timestamps[0] + 3600 - now)
            slept_seconds += sleep_for
            if self.logger:
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug(
                        "rate_limit_wait sleep_seconds=%.2f queued=%s limit_per_hour=%s",
                        sleep_for,
                        len(self._timestamps),
                        self.requests_per_hour,
                    )
                else:
                    self.logger.info("Rate limit reached. Waiting %.2fs before continuing.", sleep_for)
            time.sleep(sleep_for)


class YNABClient:
    def __init__(
        self,
        token: str,
        base_url: str = "https://api.ynab.com/v1",
        timeout_seconds: int = 90,
        rate_limit_per_hour: int = 200,
        retry_config: Optional[RetryConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.retry_config = retry_config or RetryConfig()
        self.logger = logger or logging.getLogger("ynab_migrator.client")
        self.rate_limiter = RollingRateLimiter(
            requests_per_hour=rate_limit_per_hour,
            logger=self.logger.getChild("rate_limiter"),
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "ynab-migrator/0.1",
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            rate_limit_sleep = self.rate_limiter.acquire()
            request_started_at = time.perf_counter()
            payload_summary = self._payload_summary(json_body)
            params_keys = sorted(params.keys()) if isinstance(params, dict) else []
            self.logger.debug(
                "http_request_start method=%s path=%s attempt=%s params_keys=%s payload=%s rate_limit_sleep_seconds=%.2f",
                method,
                path,
                attempt + 1,
                params_keys,
                payload_summary,
                rate_limit_sleep,
            )
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as error:
                duration_ms = int((time.perf_counter() - request_started_at) * 1000)
                self.logger.debug(
                    "http_request_exception method=%s path=%s attempt=%s duration_ms=%s error=%s",
                    method,
                    path,
                    attempt + 1,
                    duration_ms,
                    error.__class__.__name__,
                )
                if attempt >= self.retry_config.max_retries:
                    raise RuntimeError(f"request failure: {error}") from error
                sleep_seconds = self._sleep_for_retry(attempt, None)
                self.logger.warning(
                    "API request %s %s failed (%s). Retrying in %.2fs (attempt %s).",
                    method,
                    path,
                    error.__class__.__name__,
                    sleep_seconds,
                    attempt + 1,
                )
                attempt += 1
                continue

            duration_ms = int((time.perf_counter() - request_started_at) * 1000)
            if response.status_code in (429, 500, 502, 503, 504):
                self.logger.debug(
                    "http_request_transient method=%s path=%s status=%s attempt=%s duration_ms=%s",
                    method,
                    path,
                    response.status_code,
                    attempt + 1,
                    duration_ms,
                )
                if attempt >= self.retry_config.max_retries:
                    payload = self._safe_json(response)
                    raise YNABApiError(
                        response.status_code,
                        f"YNAB transient error after retries ({response.status_code})",
                        payload,
                    )
                sleep_seconds = self._sleep_for_retry(attempt, response)
                self.logger.warning(
                    "API returned transient status %s for %s %s. Retrying in %.2fs (attempt %s).",
                    response.status_code,
                    method,
                    path,
                    sleep_seconds,
                    attempt + 1,
                )
                attempt += 1
                continue

            payload = self._safe_json(response)
            if response.status_code >= 300:
                message = self._extract_error_message(payload) or response.text or "request failed"
                self.logger.error(
                    "http_request_failed method=%s path=%s status=%s attempt=%s duration_ms=%s",
                    method,
                    path,
                    response.status_code,
                    attempt + 1,
                    duration_ms,
                )
                raise YNABApiError(response.status_code, message, payload)

            self.logger.debug(
                "http_request_end method=%s path=%s status=%s attempt=%s duration_ms=%s",
                method,
                path,
                response.status_code,
                attempt + 1,
                duration_ms,
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(data, dict):
                return data
            return {}

    def _sleep_for_retry(self, attempt: int, response: Optional[requests.Response]) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_seconds = max(float(retry_after), 1.0)
                    time.sleep(sleep_seconds)
                    return sleep_seconds
                except ValueError:
                    pass

        exponential = self.retry_config.base_delay_seconds * (2 ** attempt)
        capped = min(exponential, self.retry_config.max_delay_seconds)
        jitter = random.uniform(0.0, min(capped * 0.15, 5.0))
        sleep_seconds = capped + jitter
        time.sleep(sleep_seconds)
        return sleep_seconds

    @staticmethod
    def _safe_json(response: requests.Response) -> Dict[str, Any]:
        try:
            data = response.json()
            if isinstance(data, dict):
                return data
            return {}
        except ValueError:
            return {}

    @staticmethod
    def _extract_error_message(payload: Dict[str, Any]) -> Optional[str]:
        error = payload.get("error")
        if isinstance(error, dict):
            detail = error.get("detail")
            name = error.get("name")
            if detail and name:
                return f"{name}: {detail}"
            if detail:
                return str(detail)
            if name:
                return str(name)
        return None

    @staticmethod
    def _payload_summary(json_body: Optional[Dict[str, Any]]) -> str:
        if not isinstance(json_body, dict):
            return "none"

        keys = sorted(json_body.keys())
        summary_parts = [f"keys={','.join(keys)}"]
        transactions = json_body.get("transactions")
        if isinstance(transactions, list):
            summary_parts.append(f"transactions_count={len(transactions)}")
        if isinstance(json_body.get("transaction"), dict):
            summary_parts.append("single_transaction=1")
        if isinstance(json_body.get("scheduled_transaction"), dict):
            summary_parts.append("single_scheduled_transaction=1")
        if isinstance(json_body.get("category"), dict):
            summary_parts.append("single_category=1")
        if isinstance(json_body.get("account"), dict):
            summary_parts.append("single_account=1")
        if isinstance(json_body.get("category_group"), dict):
            summary_parts.append("single_category_group=1")
        return ";".join(summary_parts)

    # Read endpoints
    def get_plan(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}")

    def get_plan_settings(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/settings")

    def get_plan_months(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/months")

    def get_plan_month(self, plan_id: str, month: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/months/{month}")

    def get_accounts(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/accounts")

    def get_categories(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/categories")

    def get_payees(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/payees")

    def get_transactions(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/transactions")

    def get_transaction(self, plan_id: str, transaction_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/transactions/{transaction_id}")

    def get_account_transactions(self, plan_id: str, account_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/accounts/{account_id}/transactions")

    def get_scheduled_transactions(self, plan_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/plans/{plan_id}/scheduled_transactions")

    # Write endpoints
    def create_account(self, plan_id: str, account: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", f"/plans/{plan_id}/accounts", json_body={"account": account})

    def create_category_group(self, plan_id: str, category_group: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/plans/{plan_id}/category_groups",
            json_body={"category_group": category_group},
        )

    def create_category(self, plan_id: str, category: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", f"/plans/{plan_id}/categories", json_body={"category": category})

    def create_transactions(self, plan_id: str, transactions: Any) -> Dict[str, Any]:
        if isinstance(transactions, list):
            body = {"transactions": transactions}
        else:
            body = {"transaction": transactions}
        return self._request("POST", f"/plans/{plan_id}/transactions", json_body=body)

    def update_transaction(self, plan_id: str, transaction_id: str, transaction: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "PUT",
            f"/plans/{plan_id}/transactions/{transaction_id}",
            json_body={"transaction": transaction},
        )

    def create_scheduled_transaction(
        self, plan_id: str, scheduled_transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/plans/{plan_id}/scheduled_transactions",
            json_body={"scheduled_transaction": scheduled_transaction},
        )

    def patch_month_category(
        self,
        plan_id: str,
        month: str,
        category_id: str,
        budgeted: int,
    ) -> Dict[str, Any]:
        return self._request(
            "PATCH",
            f"/plans/{plan_id}/months/{month}/categories/{category_id}",
            json_body={"category": {"budgeted": int(budgeted)}},
        )

    def delete_transaction(self, plan_id: str, transaction_id: str) -> Dict[str, Any]:
        return self._request("DELETE", f"/plans/{plan_id}/transactions/{transaction_id}")
