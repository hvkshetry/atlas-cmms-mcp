"""Async HTTP client for Atlas CMMS (Grashjs) REST API.

JWT-authenticated via email/password login. Auto-refreshes on 401.
All list endpoints use POST /search with SearchCriteria body.
"""

import logging
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)


class AtlasCMMSClient:
    """JWT-authenticated async client for the Grashjs REST API."""

    def __init__(self, base_url: str, email: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self.token: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def login(self) -> str:
        """Authenticate and get JWT token.

        POST /auth/signin {email, password, type: "CLIENT"} -> {accessToken}
        """
        session = await self._get_session()
        url = f"{self.base_url}/auth/signin"
        payload = {
            "email": self.email,
            "password": self.password,
            "type": "CLIENT",
        }
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"Atlas login failed ({resp.status}): {text}")
            data = await resp.json()
            self.token = data.get("accessToken") or data.get("access_token") or data.get("token")
            if not self.token:
                raise RuntimeError(f"No token in login response: {data}")
            logger.info("Atlas CMMS login successful")
            return self.token

    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def request(
        self,
        method: str,
        path: str,
        json_data: Any = None,
        params: dict = None,
        retry_auth: bool = True,
    ) -> Any:
        """Make an authenticated API request with auto-retry on 401."""
        if not self.token:
            await self.login()

        session = await self._get_session()
        url = f"{self.base_url}{path}"
        logger.debug(f"Atlas API: {method} {url}")

        async with session.request(
            method, url, json=json_data, params=params, headers=self._headers()
        ) as resp:
            if resp.status == 401 and retry_auth:
                logger.info("Atlas token expired, re-authenticating...")
                await self.login()
                return await self.request(method, path, json_data, params, retry_auth=False)

            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Atlas API error {resp.status} {method} {path}: {text}")

            if not text or text.strip() == "":
                return {}

            try:
                return await resp.json(content_type=None)
            except Exception:
                return {"raw": text}

    # ── Convenience methods ──────────────────────────────────────────

    async def get(self, path: str, params: dict = None) -> Any:
        return await self.request("GET", path, params=params)

    async def post(self, path: str, data: Any = None) -> Any:
        return await self.request("POST", path, json_data=data)

    async def patch(self, path: str, data: Any = None) -> Any:
        return await self.request("PATCH", path, json_data=data)

    async def delete(self, path: str) -> Any:
        return await self.request("DELETE", path)

    async def search(
        self,
        path: str,
        filter_fields: list = None,
        page_num: int = 0,
        page_size: int = 25,
        sort_field: str = None,
        direction: str = "DESC",
    ) -> Any:
        """POST search with SearchCriteria body (standard Grashjs pattern)."""
        criteria = {
            "filterFields": filter_fields or [],
            "pageNum": page_num,
            "pageSize": page_size,
            "direction": direction,
        }
        if sort_field:
            criteria["sortField"] = sort_field
        return await self.post(f"{path}/search", criteria)

    # ═══════════════════════════════════════════════════════════════════
    # Work Order operations
    # ═══════════════════════════════════════════════════════════════════

    async def wo_search(self, **kwargs) -> Any:
        return await self.search("/work-orders", **kwargs)

    async def wo_search_mini(self, **kwargs) -> Any:
        """POST /work-orders/search/mini (not /work-orders/mini/search)."""
        criteria = {
            "filterFields": kwargs.get("filter_fields", []),
            "pageNum": kwargs.get("page_num", 0),
            "pageSize": kwargs.get("page_size", 25),
            "direction": kwargs.get("direction", "DESC"),
        }
        if kwargs.get("sort_field"):
            criteria["sortField"] = kwargs["sort_field"]
        return await self.post("/work-orders/search/mini", criteria)

    async def wo_get(self, pk: int) -> Any:
        return await self.get(f"/work-orders/{pk}")

    async def wo_create(self, data: dict) -> Any:
        return await self.post("/work-orders", data)

    async def wo_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/work-orders/{pk}", data)

    async def wo_delete(self, pk: int) -> Any:
        return await self.delete(f"/work-orders/{pk}")

    async def wo_change_status(self, pk: int, status: str) -> Any:
        return await self.patch(f"/work-orders/{pk}/change-status", {"status": status})

    async def wo_by_asset(self, asset_id: int) -> Any:
        return await self.get(f"/work-orders/asset/{asset_id}")

    async def wo_by_location(self, location_id: int) -> Any:
        return await self.get(f"/work-orders/location/{location_id}")

    async def wo_by_part(self, part_id: int) -> Any:
        return await self.get(f"/work-orders/part/{part_id}")

    async def wo_get_report(self, pk: int) -> Any:
        return await self.get(f"/work-orders/report/{pk}")

    async def wo_get_urgent(self) -> Any:
        return await self.get("/work-orders/urgent")

    async def wo_add_files(self, pk: int, file_ids: list) -> Any:
        return await self.patch(f"/work-orders/files/{pk}/add", {"files": file_ids})

    async def wo_remove_file(self, pk: int, file_id: int) -> Any:
        return await self.delete(f"/work-orders/files/{pk}/{file_id}/remove")

    async def wo_get_history(self, pk: int) -> Any:
        return await self.get(f"/work-order-histories/work-order/{pk}")

    async def wo_get_history_entry(self, pk: int) -> Any:
        """GET /work-order-histories/{id} — get a single history entry."""
        return await self.get(f"/work-order-histories/{pk}")

    async def wo_list_categories(self) -> Any:
        return await self.get("/work-order-categories")

    async def wo_get_category(self, pk: int) -> Any:
        return await self.get(f"/work-order-categories/{pk}")

    async def wo_create_category(self, data: dict) -> Any:
        return await self.post("/work-order-categories", data)

    async def wo_delete_category(self, pk: int) -> Any:
        return await self.delete(f"/work-order-categories/{pk}")

    async def wo_get_configuration(self, pk: int) -> Any:
        """GET /work-order-configurations/{id} (read-only, no PATCH)."""
        return await self.get(f"/work-order-configurations/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Asset operations
    # ═══════════════════════════════════════════════════════════════════

    async def asset_search(self, **kwargs) -> Any:
        return await self.search("/assets", **kwargs)

    async def asset_get(self, pk: int) -> Any:
        return await self.get(f"/assets/{pk}")

    async def asset_create(self, data: dict) -> Any:
        return await self.post("/assets", data)

    async def asset_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/assets/{pk}", data)

    async def asset_delete(self, pk: int) -> Any:
        return await self.delete(f"/assets/{pk}")

    async def asset_children(self, pk: int) -> Any:
        return await self.get(f"/assets/children/{pk}")

    async def asset_by_location(self, location_id: int) -> Any:
        return await self.get(f"/assets/location/{location_id}")

    async def asset_by_part(self, part_id: int) -> Any:
        return await self.get(f"/assets/part/{part_id}")

    async def asset_get_mini(self) -> Any:
        return await self.get("/assets/mini")

    async def asset_get_by_nfc(self, nfc_id: str) -> Any:
        return await self.get("/assets/nfc", params={"nfcId": nfc_id})

    async def asset_get_by_barcode(self, barcode: str) -> Any:
        return await self.get("/assets/barcode", params={"barcode": barcode})

    async def asset_list_categories(self) -> Any:
        return await self.get("/asset-categories")

    async def asset_get_category(self, pk: int) -> Any:
        return await self.get(f"/asset-categories/{pk}")

    async def asset_create_category(self, data: dict) -> Any:
        return await self.post("/asset-categories", data)

    async def asset_delete_category(self, pk: int) -> Any:
        return await self.delete(f"/asset-categories/{pk}")

    async def asset_list_downtimes(self, asset_id: int) -> Any:
        return await self.get(f"/asset-downtimes/asset/{asset_id}")

    async def asset_create_downtime(self, data: dict) -> Any:
        return await self.post("/asset-downtimes", data)

    async def asset_update_downtime(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/asset-downtimes/{pk}", data)

    async def asset_delete_downtime(self, pk: int) -> Any:
        return await self.delete(f"/asset-downtimes/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Preventive Maintenance operations
    # ═══════════════════════════════════════════════════════════════════

    async def pm_search(self, **kwargs) -> Any:
        return await self.search("/preventive-maintenances", **kwargs)

    async def pm_get(self, pk: int) -> Any:
        return await self.get(f"/preventive-maintenances/{pk}")

    async def pm_create(self, data: dict) -> Any:
        return await self.post("/preventive-maintenances", data)

    async def pm_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/preventive-maintenances/{pk}", data)

    async def pm_delete(self, pk: int) -> Any:
        return await self.delete(f"/preventive-maintenances/{pk}")

    async def pm_list_schedules(self, page_size: int = 25) -> Any:
        """List schedules.

        Upstream Grashjs currently has GET /schedules list commented out in
        ScheduleController, but schedule objects are reachable via PMs.
        This method falls back to extracting schedules from PM search results.
        """
        pms = await self.pm_search(page_size=page_size)
        items: list[dict] = []
        if isinstance(pms, list):
            items = [p for p in pms if isinstance(p, dict)]
        elif isinstance(pms, dict):
            for key in ("content", "results", "data", "items"):
                value = pms.get(key)
                if isinstance(value, list):
                    items = [p for p in value if isinstance(p, dict)]
                    break

        schedules: list[dict] = []
        for pm in items:
            sched = pm.get("schedule")
            if isinstance(sched, dict):
                schedules.append(sched)
        return schedules

    async def pm_get_schedule(self, pk: int) -> Any:
        return await self.get(f"/schedules/{pk}")

    async def pm_update_schedule(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/schedules/{pk}", data)

    async def pm_delete_schedule(self, pk: int) -> Any:
        return await self.delete(f"/schedules/{pk}")

    async def pm_get_trigger(self, pk: int) -> Any:
        return await self.get(f"/work-order-meter-triggers/{pk}")

    async def pm_list_triggers_by_meter(self, meter_id: int) -> Any:
        """GET /work-order-meter-triggers/meter/{id} — list triggers for a meter."""
        return await self.get(f"/work-order-meter-triggers/meter/{meter_id}")

    async def pm_create_trigger(self, data: dict) -> Any:
        return await self.post("/work-order-meter-triggers", data)

    async def pm_update_trigger(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/work-order-meter-triggers/{pk}", data)

    async def pm_delete_trigger(self, pk: int) -> Any:
        return await self.delete(f"/work-order-meter-triggers/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Part (CMMS parts/inventory) operations
    # ═══════════════════════════════════════════════════════════════════

    async def part_search(self, **kwargs) -> Any:
        return await self.search("/parts", **kwargs)

    async def part_get(self, pk: int) -> Any:
        return await self.get(f"/parts/{pk}")

    async def part_create(self, data: dict) -> Any:
        return await self.post("/parts", data)

    async def part_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/parts/{pk}", data)

    async def part_delete(self, pk: int) -> Any:
        return await self.delete(f"/parts/{pk}")

    async def part_list_categories(self) -> Any:
        return await self.get("/part-categories")

    async def part_get_category(self, pk: int) -> Any:
        return await self.get(f"/part-categories/{pk}")

    async def part_create_category(self, data: dict) -> Any:
        return await self.post("/part-categories", data)

    async def part_delete_category(self, pk: int) -> Any:
        return await self.delete(f"/part-categories/{pk}")

    async def part_adjust_quantity(self, pk: int, quantity: float, operation: str = "add") -> Any:
        return await self.post(f"/part-quantities", {"part": {"id": pk}, "quantity": quantity, "type": operation})

    async def part_get_quantities_by_wo(self, wo_id: int) -> Any:
        """GET /part-quantities/work-order/{id} — quantities consumed by a work order."""
        return await self.get(f"/part-quantities/work-order/{wo_id}")

    async def part_get_quantities_by_po(self, po_id: int) -> Any:
        """GET /part-quantities/purchase-order/{id} — quantities from a purchase order."""
        return await self.get(f"/part-quantities/purchase-order/{po_id}")

    async def part_set_quantity(self, pk: int, quantity: float) -> Any:
        return await self.patch(f"/part-quantities/{pk}", {"quantity": quantity})

    async def part_list_multi_parts(self) -> Any:
        return await self.get("/multi-parts")

    async def part_get_multi_part(self, pk: int) -> Any:
        return await self.get(f"/multi-parts/{pk}")

    async def part_create_multi_part(self, data: dict) -> Any:
        return await self.post("/multi-parts", data)

    # ═══════════════════════════════════════════════════════════════════
    # Purchase Order operations
    # ═══════════════════════════════════════════════════════════════════

    async def po_search(self, **kwargs) -> Any:
        return await self.search("/purchase-orders", **kwargs)

    async def po_get(self, pk: int) -> Any:
        return await self.get(f"/purchase-orders/{pk}")

    async def po_create(self, data: dict) -> Any:
        return await self.post("/purchase-orders", data)

    async def po_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/purchase-orders/{pk}", data)

    async def po_delete(self, pk: int) -> Any:
        return await self.delete(f"/purchase-orders/{pk}")

    async def po_list_categories(self) -> Any:
        return await self.get("/purchase-order-categories")

    async def po_get_category(self, pk: int) -> Any:
        return await self.get(f"/purchase-order-categories/{pk}")

    async def po_create_category(self, data: dict) -> Any:
        return await self.post("/purchase-order-categories", data)

    async def po_delete_category(self, pk: int) -> Any:
        return await self.delete(f"/purchase-order-categories/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Meter operations
    # ═══════════════════════════════════════════════════════════════════

    async def meter_search(self, **kwargs) -> Any:
        return await self.search("/meters", **kwargs)

    async def meter_get(self, pk: int) -> Any:
        return await self.get(f"/meters/{pk}")

    async def meter_create(self, data: dict) -> Any:
        return await self.post("/meters", data)

    async def meter_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/meters/{pk}", data)

    async def meter_delete(self, pk: int) -> Any:
        return await self.delete(f"/meters/{pk}")

    async def meter_list_categories(self) -> Any:
        return await self.get("/meter-categories")

    async def meter_get_category(self, pk: int) -> Any:
        return await self.get(f"/meter-categories/{pk}")

    async def meter_add_reading(self, data: dict) -> Any:
        return await self.post("/readings", data)

    async def meter_get_readings(self, meter_id: int) -> Any:
        return await self.get(f"/readings/meter/{meter_id}")

    async def meter_delete_reading(self, pk: int) -> Any:
        return await self.delete(f"/readings/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Location operations
    # ═══════════════════════════════════════════════════════════════════

    async def location_search(self, **kwargs) -> Any:
        return await self.search("/locations", **kwargs)

    async def location_get(self, pk: int) -> Any:
        return await self.get(f"/locations/{pk}")

    async def location_create(self, data: dict) -> Any:
        return await self.post("/locations", data)

    async def location_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/locations/{pk}", data)

    async def location_delete(self, pk: int) -> Any:
        return await self.delete(f"/locations/{pk}")

    async def location_by_parent(self, parent_id: int) -> Any:
        return await self.get(f"/locations/children/{parent_id}")

    async def location_list_floor_plans(self, location_id: int) -> Any:
        """GET /floor-plans/location/{id} — floor plans for a location."""
        return await self.get(f"/floor-plans/location/{location_id}")

    async def location_get_floor_plan(self, pk: int) -> Any:
        return await self.get(f"/floor-plans/{pk}")

    async def location_create_floor_plan(self, data: dict) -> Any:
        return await self.post("/floor-plans", data)

    async def location_update_floor_plan(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/floor-plans/{pk}", data)

    async def location_delete_floor_plan(self, pk: int) -> Any:
        return await self.delete(f"/floor-plans/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Vendor & Customer operations
    # ═══════════════════════════════════════════════════════════════════

    async def vendor_search(self, **kwargs) -> Any:
        return await self.search("/vendors", **kwargs)

    async def vendor_get(self, pk: int) -> Any:
        return await self.get(f"/vendors/{pk}")

    async def vendor_create(self, data: dict) -> Any:
        return await self.post("/vendors", data)

    async def vendor_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/vendors/{pk}", data)

    async def vendor_delete(self, pk: int) -> Any:
        return await self.delete(f"/vendors/{pk}")

    async def customer_search(self, **kwargs) -> Any:
        return await self.search("/customers", **kwargs)

    async def customer_get(self, pk: int) -> Any:
        return await self.get(f"/customers/{pk}")

    async def customer_create(self, data: dict) -> Any:
        return await self.post("/customers", data)

    async def customer_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/customers/{pk}", data)

    async def customer_delete(self, pk: int) -> Any:
        return await self.delete(f"/customers/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Team, User & Role operations
    # ═══════════════════════════════════════════════════════════════════

    async def team_search(self, **kwargs) -> Any:
        return await self.search("/teams", **kwargs)

    async def team_get(self, pk: int) -> Any:
        return await self.get(f"/teams/{pk}")

    async def team_create(self, data: dict) -> Any:
        return await self.post("/teams", data)

    async def team_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/teams/{pk}", data)

    async def team_delete(self, pk: int) -> Any:
        return await self.delete(f"/teams/{pk}")

    async def user_search(self, **kwargs) -> Any:
        """POST /users/search — search users with SearchCriteria."""
        return await self.search("/users", **kwargs)

    async def user_get(self, pk: int) -> Any:
        return await self.get(f"/users/{pk}")

    async def user_invite(self, data: dict) -> Any:
        """POST /users/invite — invite (create) a new user."""
        return await self.post("/users/invite", data)

    async def user_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/users/{pk}", data)

    async def user_disable(self, pk: int) -> Any:
        """PATCH /users/{id}/disable — disable (lock) a user account."""
        return await self.patch(f"/users/{pk}/disable")

    async def user_soft_delete(self, pk: int) -> Any:
        """PATCH /users/soft-delete/{id} — soft-delete a user."""
        return await self.patch(f"/users/soft-delete/{pk}")

    async def role_list(self) -> Any:
        return await self.get("/roles")

    async def role_get(self, pk: int) -> Any:
        return await self.get(f"/roles/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Task & Checklist operations
    # ═══════════════════════════════════════════════════════════════════

    async def task_get(self, pk: int) -> Any:
        return await self.get(f"/tasks/{pk}")

    async def task_list_by_wo(self, wo_id: int) -> Any:
        """GET /tasks/work-order/{id} — list tasks for a work order."""
        return await self.get(f"/tasks/work-order/{wo_id}")

    async def task_list_by_pm(self, pm_id: int) -> Any:
        """GET /tasks/preventive-maintenance/{id} — list tasks for a PM."""
        return await self.get(f"/tasks/preventive-maintenance/{pm_id}")

    async def task_set_wo_tasks(self, wo_id: int, data: list) -> Any:
        """PATCH /tasks/work-order/{id} — set tasks on a work order."""
        return await self.patch(f"/tasks/work-order/{wo_id}", data)

    async def task_set_pm_tasks(self, pm_id: int, data: list) -> Any:
        """PATCH /tasks/preventive-maintenance/{id} — set tasks on a PM."""
        return await self.patch(f"/tasks/preventive-maintenance/{pm_id}", data)

    async def task_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/tasks/{pk}", data)

    async def task_delete(self, pk: int) -> Any:
        return await self.delete(f"/tasks/{pk}")

    async def checklist_list(self) -> Any:
        """GET /checklists — list all checklists."""
        return await self.get("/checklists")

    async def checklist_get(self, pk: int) -> Any:
        return await self.get(f"/checklists/{pk}")

    async def checklist_create(self, data: dict) -> Any:
        return await self.post("/checklists", data)

    async def checklist_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/checklists/{pk}", data)

    async def checklist_delete(self, pk: int) -> Any:
        return await self.delete(f"/checklists/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Request operations
    # ═══════════════════════════════════════════════════════════════════

    async def request_search(self, **kwargs) -> Any:
        return await self.search("/requests", **kwargs)

    async def request_get(self, pk: int) -> Any:
        return await self.get(f"/requests/{pk}")

    async def request_create(self, data: dict) -> Any:
        return await self.post("/requests", data)

    async def request_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/requests/{pk}", data)

    async def request_delete(self, pk: int) -> Any:
        return await self.delete(f"/requests/{pk}")

    async def request_approve(self, pk: int, data: dict = None) -> Any:
        """PATCH /requests/{id}/approve — approve a work order request."""
        return await self.patch(f"/requests/{pk}/approve", data or {})

    async def request_cancel(self, pk: int, reason: str = "") -> Any:
        """PATCH /requests/{id}/cancel — cancel (reject) a work order request."""
        params = {"reason": reason} if reason else None
        return await self.request("PATCH", f"/requests/{pk}/cancel", params=params)

    async def request_get_configuration(self, pk: int) -> Any:
        """GET /work-order-request-configurations/{id} (read-only)."""
        return await self.get(f"/work-order-request-configurations/{pk}")

    # ═══════════════════════════════════════════════════════════════════
    # Cost & Labor operations
    # ═══════════════════════════════════════════════════════════════════

    async def cost_list(self, wo_id: int) -> Any:
        """GET /additional-costs/work-order/{id} — costs for a work order."""
        return await self.get(f"/additional-costs/work-order/{wo_id}")

    async def cost_create(self, data: dict) -> Any:
        return await self.post("/additional-costs", data)

    async def cost_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/additional-costs/{pk}", data)

    async def cost_delete(self, pk: int) -> Any:
        return await self.delete(f"/additional-costs/{pk}")

    async def labor_list(self, wo_id: int) -> Any:
        """GET /labors/work-order/{id} — labor entries for a work order."""
        return await self.get(f"/labors/work-order/{wo_id}")

    async def labor_create(self, data: dict) -> Any:
        return await self.post("/labors", data)

    async def labor_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/labors/{pk}", data)

    async def labor_delete(self, pk: int) -> Any:
        return await self.delete(f"/labors/{pk}")

    async def cost_list_categories(self) -> Any:
        return await self.get("/cost-categories")

    async def time_list_categories(self) -> Any:
        return await self.get("/time-categories")

    # ═══════════════════════════════════════════════════════════════════
    # Analytics operations (all POST with DateRange body)
    # ═══════════════════════════════════════════════════════════════════

    async def _analytics(self, path: str, start: str = None, end: str = None) -> Any:
        """Helper for analytics endpoints. All take POST with date range."""
        data = {}
        if start:
            data["start"] = start
        if end:
            data["end"] = end
        return await self.post(path, data)

    # WO Analytics
    async def analytics_wo_complete_overview(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/complete/overview", **kw)

    async def analytics_wo_incomplete_overview(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/incomplete/overview", **kw)

    async def analytics_wo_priority(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/incomplete/priority", **kw)

    async def analytics_wo_statuses(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/statuses", **kw)

    async def analytics_wo_hours(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/hours", **kw)

    async def analytics_wo_counts_by_user(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/complete/counts/primaryUser", **kw)

    async def analytics_wo_counts_by_priority(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/complete/counts/priority", **kw)

    async def analytics_wo_costs_time(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/complete/costs-time", **kw)

    async def analytics_wo_costs_date(self, **kw) -> Any:
        return await self._analytics("/analytics/work-orders/complete/costs/date", **kw)

    # Asset Analytics
    async def analytics_asset_time_cost(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/time-cost", **kw)

    async def analytics_asset_overview(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/overview", **kw)

    async def analytics_asset_downtimes(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/downtimes", **kw)

    async def analytics_asset_mtbf(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/mtbf", **kw)

    async def analytics_asset_meantimes(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/meantimes", **kw)

    async def analytics_asset_repair_times(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/repair-times", **kw)

    async def analytics_asset_downtime_costs(self, **kw) -> Any:
        return await self._analytics("/analytics/assets/downtimes/costs", **kw)

    async def analytics_asset_single_overview(self, asset_id: int, **kw) -> Any:
        return await self._analytics(f"/analytics/assets/{asset_id}/overview", **kw)

    # Part, Request, User Analytics
    async def analytics_part_consumptions(self, **kw) -> Any:
        """POST /analytics/parts/consumptions/overview."""
        return await self._analytics("/analytics/parts/consumptions/overview", **kw)

    async def analytics_request_overview(self, **kw) -> Any:
        return await self._analytics("/analytics/requests/overview", **kw)

    async def analytics_user_overview(self) -> Any:
        """GET /analytics/users/me/work-orders/overview (user-specific, no date range)."""
        return await self.get("/analytics/users/me/work-orders/overview")

    # ═══════════════════════════════════════════════════════════════════
    # File, Import & Export operations
    # ═══════════════════════════════════════════════════════════════════

    async def file_search(self, filter_fields: list = None, **kwargs) -> Any:
        """POST /files/search — search files with SearchCriteria."""
        return await self.search("/files", filter_fields=filter_fields, **kwargs)

    async def file_get(self, pk: int) -> Any:
        return await self.get(f"/files/{pk}")

    async def file_update(self, pk: int, data: dict) -> Any:
        return await self.patch(f"/files/{pk}", data)

    async def file_delete(self, pk: int) -> Any:
        return await self.delete(f"/files/{pk}")

    async def file_upload(
        self,
        file_path: str,
        folder: str = "mcp",
        hidden: bool = False,
        file_type: str = "OTHER",
        task_id: int | None = None,
    ) -> Any:
        """POST /files/upload — multipart file upload.

        Backend expects multipart fields:
        - files: MultipartFile[] (field name "files")
        - folder: string
        - hidden: "true"/"false"
        - type: FileType enum ("IMAGE" | "OTHER")
        - taskId: optional
        """
        import aiohttp as _aiohttp
        session = await self._get_session()
        url = f"{self.base_url}/files/upload"
        data = _aiohttp.FormData()
        filename = file_path.split("/")[-1]
        # The backend expects multipart field name "files" (plural).
        with open(file_path, "rb") as f:
            data.add_field("files", f, filename=filename)
            data.add_field("folder", folder)
            data.add_field("hidden", "true" if hidden else "false")
            data.add_field("type", file_type)
            if task_id is not None:
                data.add_field("taskId", str(task_id))
            headers = {"Authorization": f"Bearer {self.token}"}
            async with session.post(url, data=data, headers=headers) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    raise RuntimeError(f"File upload failed ({resp.status}): {text}")
                return await resp.json(content_type=None)

    async def export_work_orders(self) -> Any:
        return await self.get("/export/work-orders")

    async def export_assets(self) -> Any:
        return await self.get("/export/assets")

    async def export_parts(self) -> Any:
        return await self.get("/export/parts")

    async def import_work_orders(self, data: list) -> Any:
        return await self.post("/import/work-orders", data)

    async def import_assets(self, data: list) -> Any:
        return await self.post("/import/assets", data)

    async def import_parts(self, data: list) -> Any:
        return await self.post("/import/parts", data)

    # ═══════════════════════════════════════════════════════════════════
    # System, Auth & Config operations
    # ═══════════════════════════════════════════════════════════════════

    async def health_check(self) -> Any:
        session = await self._get_session()
        url = f"{self.base_url}/health-check"
        async with session.get(url) as resp:
            return {"status": "ok" if resp.status == 200 else "error", "code": resp.status}

    async def get_me(self) -> Any:
        return await self.get("/auth/me")

    async def refresh_token(self) -> Any:
        """GET /auth/refresh — refresh JWT token."""
        result = await self.get("/auth/refresh")
        if isinstance(result, dict) and "accessToken" in result:
            self.token = result["accessToken"]
        return result

    async def get_company(self, pk: int) -> Any:
        """GET /companies/{id}."""
        return await self.get(f"/companies/{pk}")

    async def update_company(self, pk: int, data: dict) -> Any:
        """PATCH /companies/{id}."""
        return await self.patch(f"/companies/{pk}", data)

    async def get_settings(self, pk: int) -> Any:
        """GET /company-settings/{id} (read-only)."""
        return await self.get(f"/company-settings/{pk}")

    async def get_preferences(self) -> Any:
        """GET /general-preferences."""
        return await self.get("/general-preferences")

    async def get_preferences_by_id(self, pk: int) -> Any:
        """GET /general-preferences/{id}."""
        return await self.get(f"/general-preferences/{pk}")

    async def update_preferences(self, pk: int, data: dict) -> Any:
        """PATCH /general-preferences/{id}."""
        return await self.patch(f"/general-preferences/{pk}", data)

    async def list_currencies(self) -> Any:
        return await self.get("/currencies")

    async def get_custom_field(self, pk: int) -> Any:
        """GET /custom-fields/{id}."""
        return await self.get(f"/custom-fields/{pk}")

    async def create_custom_field(self, data: dict) -> Any:
        return await self.post("/custom-fields", data)

    async def custom_field_delete(self, pk: int) -> Any:
        return await self.delete(f"/custom-fields/{pk}")

    async def update_field_config(self, pk: int, data: dict) -> Any:
        """PATCH /field-configurations/{id}."""
        return await self.patch(f"/field-configurations/{pk}", data)

    async def list_relations(self) -> Any:
        return await self.get("/relations")

    async def create_relation(self, data: dict) -> Any:
        return await self.post("/relations", data)

    async def relation_delete(self, pk: int) -> Any:
        return await self.delete(f"/relations/{pk}")

    async def list_workflows(self) -> Any:
        return await self.get("/workflows")
