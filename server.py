#!/usr/bin/env python3
"""Atlas CMMS MCP Server — 15 parameterized tools, 194 operations.

Dual transport: STDIO (default) or SSE (pass 'sse' argument).
Uses aiohttp client with JWT auth against the Grashjs REST API.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP("atlas_cmms_mcp")

# ── Lazy client singleton ────────────────────────────────────────────

_client = None
_client_lock = asyncio.Lock()


async def get_client():
    """Get or create the Atlas CMMS client singleton."""
    global _client
    if _client is not None:
        return _client
    async with _client_lock:
        if _client is not None:
            return _client
        from client import AtlasCMMSClient

        url = os.getenv("ATLAS_URL")
        email = os.getenv("ATLAS_EMAIL")
        password = os.getenv("ATLAS_PASSWORD")
        if not url or not email or not password:
            raise RuntimeError(
                "ATLAS_URL, ATLAS_EMAIL, and ATLAS_PASSWORD must be set in environment"
            )
        _client = AtlasCMMSClient(url, email, password)
        await _client.login()
        logger.info(f"Atlas CMMS client connected to {url}")
    return _client


def _json(data: Any) -> str:
    """Serialize response data to JSON string."""
    return json.dumps(data, indent=2, default=str)


def _error(e: Exception, context: str = "") -> str:
    """Format an actionable error message for the agent."""
    msg = str(e)
    if "401" in msg or "unauthorized" in msg.lower():
        hint = "JWT token expired or invalid. The client will auto-refresh; if this persists, check ATLAS_EMAIL/ATLAS_PASSWORD."
    elif "404" in msg or "not found" in msg.lower():
        hint = "Resource not found. Use the search operation to find valid IDs."
    elif "403" in msg or "forbidden" in msg.lower():
        hint = "Insufficient permissions. Check the user's role in Atlas CMMS."
    elif "400" in msg or "bad request" in msg.lower():
        hint = "Invalid request data. Check required fields in the 'data' parameter."
    elif "timeout" in msg.lower() or "connect" in msg.lower():
        hint = "Connection failed. Verify ATLAS_URL is reachable."
    elif "ATLAS_URL" in msg or "ATLAS_EMAIL" in msg:
        hint = "Set ATLAS_URL, ATLAS_EMAIL, and ATLAS_PASSWORD environment variables."
    else:
        hint = "Check the operation name and parameters."
    prefix = f"[{context}] " if context else ""
    logger.error(f"{prefix}{msg}")
    return _json({"error": f"{prefix}{msg}", "hint": hint})


def _safe(tool_name: str):
    """Decorator to add error handling to tool functions."""
    import functools

    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                op = kwargs.get("operation", args[0] if args else "unknown")
                return _error(e, f"{tool_name}.{op}")
        return wrapper
    return decorator


def _search_kwargs(
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> dict:
    """Build search kwargs from common parameters."""
    kw = {"page_num": page_num, "page_size": page_size, "direction": direction}
    if filter_fields:
        kw["filter_fields"] = filter_fields
    if sort_field:
        kw["sort_field"] = sort_field
    return kw


# ═══════════════════════════════════════════════════════════════════════
# Tool 1: Work Order (20 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Work Order Management",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("work_order")
async def work_order(
    operation: str,
    pk: int = None,
    data: dict = None,
    asset_id: int = None,
    location_id: int = None,
    part_id: int = None,
    file_ids: list = None,
    file_id: int = None,
    status: str = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Work order full lifecycle management in Atlas CMMS.

    Operations:
      Read: search, search_mini, get, by_asset, by_location, by_part,
        get_report, get_urgent, get_history, get_history_entry,
        list_categories, get_category, get_configuration
      Write: create, update, change_status, add_files, remove_file,
        create_category
      Delete: delete

    Args:
        operation: One of the operations listed above.
        pk: Work order or category ID.
        data: Dict of fields for create/update operations.
        asset_id: Asset ID for by_asset.
        location_id: Location ID for by_location.
        part_id: Part ID for by_part.
        file_ids: List of file IDs for add_files.
        file_id: Single file ID for remove_file.
        status: Target status for change_status (OPEN, IN_PROGRESS, ON_HOLD, COMPLETE).
        filter_fields: SearchCriteria filters (e.g. [{"field":"status","operation":"eq","value":"OPEN"}]).
        page_num/page_size: Pagination (default 0/25).
        sort_field/direction: Sort control (default DESC).

    Returns:
        JSON string with work order data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.wo_search(**kw))
    elif operation == "search_mini":
        return _json(await c.wo_search_mini(**kw))
    elif operation == "get":
        return _json(await c.wo_get(pk))
    elif operation == "create":
        return _json(await c.wo_create(data or {}))
    elif operation == "update":
        return _json(await c.wo_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.wo_delete(pk))
    elif operation == "change_status":
        return _json(await c.wo_change_status(pk, status))
    elif operation == "by_asset":
        return _json(await c.wo_by_asset(asset_id))
    elif operation == "by_location":
        return _json(await c.wo_by_location(location_id))
    elif operation == "by_part":
        return _json(await c.wo_by_part(part_id))
    elif operation == "get_report":
        return _json(await c.wo_get_report(pk))
    elif operation == "get_urgent":
        return _json(await c.wo_get_urgent())
    elif operation == "add_files":
        return _json(await c.wo_add_files(pk, file_ids or []))
    elif operation == "remove_file":
        return _json(await c.wo_remove_file(pk, file_id))
    elif operation == "get_history":
        return _json(await c.wo_get_history(pk))
    elif operation == "get_history_entry":
        return _json(await c.wo_get_history_entry(pk))
    elif operation == "list_categories":
        return _json(await c.wo_list_categories())
    elif operation == "get_category":
        return _json(await c.wo_get_category(pk))
    elif operation == "create_category":
        return _json(await c.wo_create_category(data or {}))
    elif operation == "get_configuration":
        return _json(await c.wo_get_configuration(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 2: Asset (18 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Asset Management",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("asset")
async def asset(
    operation: str,
    pk: int = None,
    data: dict = None,
    asset_id: int = None,
    location_id: int = None,
    part_id: int = None,
    nfc_id: str = None,
    barcode: str = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Asset hierarchy, tracking, categories, and downtime management in Atlas CMMS.

    Operations:
      Read: search, get, children, by_location, by_part, get_mini,
        get_by_nfc, get_by_barcode, list_categories, get_category, list_downtimes
      Write: create, update, create_category, create_downtime, update_downtime
      Delete: delete, delete_downtime

    Args:
        operation: One of the operations listed above.
        pk: Asset, category, or downtime ID.
        data: Dict of fields for create/update.
        asset_id: Asset ID for children/list_downtimes.
        location_id: Location ID for by_location.
        part_id: Part ID for by_part.
        nfc_id: NFC tag ID for get_by_nfc.
        barcode: Barcode string for get_by_barcode.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with asset data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.asset_search(**kw))
    elif operation == "get":
        return _json(await c.asset_get(pk))
    elif operation == "create":
        return _json(await c.asset_create(data or {}))
    elif operation == "update":
        return _json(await c.asset_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.asset_delete(pk))
    elif operation == "children":
        return _json(await c.asset_children(pk or asset_id))
    elif operation == "by_location":
        return _json(await c.asset_by_location(location_id))
    elif operation == "by_part":
        return _json(await c.asset_by_part(part_id))
    elif operation == "get_mini":
        return _json(await c.asset_get_mini())
    elif operation == "get_by_nfc":
        return _json(await c.asset_get_by_nfc(nfc_id))
    elif operation == "get_by_barcode":
        return _json(await c.asset_get_by_barcode(barcode))
    elif operation == "list_categories":
        return _json(await c.asset_list_categories())
    elif operation == "get_category":
        return _json(await c.asset_get_category(pk))
    elif operation == "create_category":
        return _json(await c.asset_create_category(data or {}))
    elif operation == "list_downtimes":
        return _json(await c.asset_list_downtimes(asset_id or pk))
    elif operation == "create_downtime":
        return _json(await c.asset_create_downtime(data or {}))
    elif operation == "update_downtime":
        return _json(await c.asset_update_downtime(pk, data or {}))
    elif operation == "delete_downtime":
        return _json(await c.asset_delete_downtime(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 3: Preventive Maintenance (14 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Preventive Maintenance",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("preventive_maintenance")
async def preventive_maintenance(
    operation: str,
    pk: int = None,
    data: dict = None,
    meter_id: int = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Preventive maintenance schedules, triggers, and meter-based WO generation in Atlas CMMS.

    Operations:
      Read: search, get, list_schedules, get_schedule, get_trigger,
        list_triggers_by_meter
      Write: create, update, update_schedule, create_trigger, update_trigger
      Delete: delete, delete_schedule, delete_trigger

    Args:
        operation: One of the operations listed above.
        pk: PM, schedule, or trigger ID.
        data: Dict of fields for create/update.
        meter_id: Meter ID for list_triggers_by_meter.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with PM data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.pm_search(**kw))
    elif operation == "get":
        return _json(await c.pm_get(pk))
    elif operation == "create":
        return _json(await c.pm_create(data or {}))
    elif operation == "update":
        return _json(await c.pm_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.pm_delete(pk))
    elif operation == "list_schedules":
        return _json(await c.pm_list_schedules())
    elif operation == "get_schedule":
        return _json(await c.pm_get_schedule(pk))
    elif operation == "update_schedule":
        return _json(await c.pm_update_schedule(pk, data or {}))
    elif operation == "delete_schedule":
        return _json(await c.pm_delete_schedule(pk))
    elif operation == "get_trigger":
        return _json(await c.pm_get_trigger(pk))
    elif operation == "list_triggers_by_meter":
        return _json(await c.pm_list_triggers_by_meter(meter_id or pk))
    elif operation == "create_trigger":
        return _json(await c.pm_create_trigger(data or {}))
    elif operation == "update_trigger":
        return _json(await c.pm_update_trigger(pk, data or {}))
    elif operation == "delete_trigger":
        return _json(await c.pm_delete_trigger(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 4: Part (CMMS parts/inventory) (15 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Part & Inventory Management",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("cmms_part")
async def cmms_part(
    operation: str,
    pk: int = None,
    data: dict = None,
    quantity: float = None,
    adjust_type: str = "add",
    wo_id: int = None,
    po_id: int = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """CMMS parts, inventory quantities, categories, and multi-part assemblies in Atlas CMMS.

    Operations:
      Read: search, get, list_categories, get_category, get_quantities_by_wo,
        get_quantities_by_po, list_multi_parts, get_multi_part
      Write: create, update, create_category, adjust_quantity, set_quantity,
        create_multi_part
      Delete: delete

    Args:
        operation: One of the operations listed above.
        pk: Part, category, or multi-part ID.
        data: Dict of fields for create/update.
        quantity: Quantity for adjust_quantity/set_quantity.
        adjust_type: "add" or "remove" for adjust_quantity (default "add").
        wo_id: Work order ID for get_quantities_by_wo.
        po_id: Purchase order ID for get_quantities_by_po.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with part data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.part_search(**kw))
    elif operation == "get":
        return _json(await c.part_get(pk))
    elif operation == "create":
        return _json(await c.part_create(data or {}))
    elif operation == "update":
        return _json(await c.part_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.part_delete(pk))
    elif operation == "list_categories":
        return _json(await c.part_list_categories())
    elif operation == "get_category":
        return _json(await c.part_get_category(pk))
    elif operation == "create_category":
        return _json(await c.part_create_category(data or {}))
    elif operation == "adjust_quantity":
        return _json(await c.part_adjust_quantity(pk, quantity, adjust_type))
    elif operation == "get_quantities_by_wo":
        return _json(await c.part_get_quantities_by_wo(wo_id or pk))
    elif operation == "get_quantities_by_po":
        return _json(await c.part_get_quantities_by_po(po_id or pk))
    elif operation == "set_quantity":
        return _json(await c.part_set_quantity(pk, quantity))
    elif operation == "list_multi_parts":
        return _json(await c.part_list_multi_parts())
    elif operation == "get_multi_part":
        return _json(await c.part_get_multi_part(pk))
    elif operation == "create_multi_part":
        return _json(await c.part_create_multi_part(data or {}))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 5: Purchase Order (8 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Purchase Order Management",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("cmms_purchase_order")
async def cmms_purchase_order(
    operation: str,
    pk: int = None,
    data: dict = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """CMMS purchase order lifecycle and categories in Atlas CMMS.

    Operations:
      Read: search, get, list_categories, get_category
      Write: create, update, create_category
      Delete: delete

    Args:
        operation: One of the operations listed above.
        pk: Purchase order or category ID.
        data: Dict of fields for create/update.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with PO data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.po_search(**kw))
    elif operation == "get":
        return _json(await c.po_get(pk))
    elif operation == "create":
        return _json(await c.po_create(data or {}))
    elif operation == "update":
        return _json(await c.po_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.po_delete(pk))
    elif operation == "list_categories":
        return _json(await c.po_list_categories())
    elif operation == "get_category":
        return _json(await c.po_get_category(pk))
    elif operation == "create_category":
        return _json(await c.po_create_category(data or {}))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 6: Meter (10 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Meter & Readings",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("meter")
async def meter(
    operation: str,
    pk: int = None,
    data: dict = None,
    meter_id: int = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Meters, categories, readings, and condition monitoring in Atlas CMMS.

    Operations:
      Read: search, get, list_categories, get_category, get_readings
      Write: create, update, add_reading
      Delete: delete, delete_reading

    Args:
        operation: One of the operations listed above.
        pk: Meter, category, or reading ID.
        data: Dict of fields for create/update/add_reading.
        meter_id: Meter ID for get_readings.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with meter data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.meter_search(**kw))
    elif operation == "get":
        return _json(await c.meter_get(pk))
    elif operation == "create":
        return _json(await c.meter_create(data or {}))
    elif operation == "update":
        return _json(await c.meter_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.meter_delete(pk))
    elif operation == "list_categories":
        return _json(await c.meter_list_categories())
    elif operation == "get_category":
        return _json(await c.meter_get_category(pk))
    elif operation == "add_reading":
        return _json(await c.meter_add_reading(data or {}))
    elif operation == "get_readings":
        return _json(await c.meter_get_readings(meter_id or pk))
    elif operation == "delete_reading":
        return _json(await c.meter_delete_reading(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 7: Location (11 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Location & Floor Plans",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("location")
async def location(
    operation: str,
    pk: int = None,
    data: dict = None,
    parent_id: int = None,
    location_id: int = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Location hierarchy and floor plan management in Atlas CMMS.

    Operations:
      Read: search, get, by_parent, list_floor_plans, get_floor_plan
      Write: create, update, create_floor_plan, update_floor_plan
      Delete: delete, delete_floor_plan

    Args:
        operation: One of the operations listed above.
        pk: Location or floor plan ID.
        data: Dict of fields for create/update.
        parent_id: Parent location ID for by_parent.
        location_id: Location ID for list_floor_plans.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with location data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.location_search(**kw))
    elif operation == "get":
        return _json(await c.location_get(pk))
    elif operation == "create":
        return _json(await c.location_create(data or {}))
    elif operation == "update":
        return _json(await c.location_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.location_delete(pk))
    elif operation == "by_parent":
        return _json(await c.location_by_parent(parent_id or pk))
    elif operation == "list_floor_plans":
        return _json(await c.location_list_floor_plans(location_id or pk))
    elif operation == "get_floor_plan":
        return _json(await c.location_get_floor_plan(pk))
    elif operation == "create_floor_plan":
        return _json(await c.location_create_floor_plan(data or {}))
    elif operation == "update_floor_plan":
        return _json(await c.location_update_floor_plan(pk, data or {}))
    elif operation == "delete_floor_plan":
        return _json(await c.location_delete_floor_plan(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 8: Vendor (10 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Vendor & Customer Management",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("vendor")
async def vendor(
    operation: str,
    pk: int = None,
    data: dict = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Vendor and customer management in Atlas CMMS.

    Operations:
      Read: search_vendors, get_vendor, search_customers, get_customer
      Write: create_vendor, update_vendor, create_customer, update_customer
      Delete: delete_vendor, delete_customer

    Args:
        operation: One of the operations listed above.
        pk: Vendor or customer ID.
        data: Dict of fields for create/update.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with vendor/customer data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search_vendors":
        return _json(await c.vendor_search(**kw))
    elif operation == "get_vendor":
        return _json(await c.vendor_get(pk))
    elif operation == "create_vendor":
        return _json(await c.vendor_create(data or {}))
    elif operation == "update_vendor":
        return _json(await c.vendor_update(pk, data or {}))
    elif operation == "delete_vendor":
        return _json(await c.vendor_delete(pk))
    elif operation == "search_customers":
        return _json(await c.customer_search(**kw))
    elif operation == "get_customer":
        return _json(await c.customer_get(pk))
    elif operation == "create_customer":
        return _json(await c.customer_create(data or {}))
    elif operation == "update_customer":
        return _json(await c.customer_update(pk, data or {}))
    elif operation == "delete_customer":
        return _json(await c.customer_delete(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 9: Team (12 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Team & User Management",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("team")
async def team(
    operation: str,
    pk: int = None,
    data: dict = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Team, user, and role management in Atlas CMMS.

    Operations:
      Read: search_teams, get_team, search_users, get_user, list_roles, get_role
      Write: create_team, update_team, invite_user, update_user, disable_user
      Delete: soft_delete_user

    Args:
        operation: One of the operations listed above.
        pk: Team, user, or role ID.
        data: Dict of fields for create/update/invite.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with team/user/role data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search_teams":
        return _json(await c.team_search(**kw))
    elif operation == "get_team":
        return _json(await c.team_get(pk))
    elif operation == "create_team":
        return _json(await c.team_create(data or {}))
    elif operation == "update_team":
        return _json(await c.team_update(pk, data or {}))
    elif operation == "search_users":
        return _json(await c.user_search(**kw))
    elif operation == "get_user":
        return _json(await c.user_get(pk))
    elif operation == "invite_user":
        return _json(await c.user_invite(data or {}))
    elif operation == "update_user":
        return _json(await c.user_update(pk, data or {}))
    elif operation == "soft_delete_user":
        return _json(await c.user_soft_delete(pk))
    elif operation == "disable_user":
        return _json(await c.user_disable(pk))
    elif operation == "list_roles":
        return _json(await c.role_list())
    elif operation == "get_role":
        return _json(await c.role_get(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 10: Task (12 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Tasks & Checklists",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("task")
async def task(
    operation: str,
    pk: int = None,
    data: dict = None,
    wo_id: int = None,
    pm_id: int = None,
    tasks_data: list = None,
) -> str:
    """Tasks and checklists for work orders and PMs in Atlas CMMS.

    Operations:
      Read: get_task, list_by_wo, list_by_pm, list_checklists, get_checklist
      Write: update_task, set_wo_tasks, set_pm_tasks, create_checklist, update_checklist
      Delete: delete_task, delete_checklist

    Args:
        operation: One of the operations listed above.
        pk: Task or checklist ID.
        data: Dict of fields for update_task/create_checklist/update_checklist.
        wo_id: Work order ID for list_by_wo/set_wo_tasks.
        pm_id: PM ID for list_by_pm/set_pm_tasks.
        tasks_data: List of task dicts for set_wo_tasks/set_pm_tasks.

    Returns:
        JSON string with task/checklist data or {"error": "..."}.
    """
    c = await get_client()

    if operation == "get_task":
        return _json(await c.task_get(pk))
    elif operation == "list_by_wo":
        return _json(await c.task_list_by_wo(wo_id or pk))
    elif operation == "list_by_pm":
        return _json(await c.task_list_by_pm(pm_id or pk))
    elif operation == "set_wo_tasks":
        return _json(await c.task_set_wo_tasks(wo_id or pk, tasks_data or []))
    elif operation == "set_pm_tasks":
        return _json(await c.task_set_pm_tasks(pm_id or pk, tasks_data or []))
    elif operation == "update_task":
        return _json(await c.task_update(pk, data or {}))
    elif operation == "delete_task":
        return _json(await c.task_delete(pk))
    elif operation == "list_checklists":
        return _json(await c.checklist_list())
    elif operation == "get_checklist":
        return _json(await c.checklist_get(pk))
    elif operation == "create_checklist":
        return _json(await c.checklist_create(data or {}))
    elif operation == "update_checklist":
        return _json(await c.checklist_update(pk, data or {}))
    elif operation == "delete_checklist":
        return _json(await c.checklist_delete(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 11: Request (8 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Work Order Requests",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("request")
async def request(
    operation: str,
    pk: int = None,
    data: dict = None,
    reason: str = "",
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """Work order request management and approval workflows in Atlas CMMS.

    Operations:
      Read: search, get, get_configuration
      Write: create, update, approve, cancel
      Delete: delete

    Args:
        operation: One of the operations listed above.
        pk: Request or configuration ID.
        data: Dict of fields for create/update/approve.
        reason: Reason string for cancel operation.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with request data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.request_search(**kw))
    elif operation == "get":
        return _json(await c.request_get(pk))
    elif operation == "create":
        return _json(await c.request_create(data or {}))
    elif operation == "update":
        return _json(await c.request_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.request_delete(pk))
    elif operation == "approve":
        return _json(await c.request_approve(pk, data))
    elif operation == "cancel":
        return _json(await c.request_cancel(pk, reason))
    elif operation == "get_configuration":
        return _json(await c.request_get_configuration(pk))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 12: Cost (10 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Cost & Labor Tracking",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("cost")
async def cost(
    operation: str,
    pk: int = None,
    data: dict = None,
    wo_id: int = None,
) -> str:
    """Cost tracking, labor, and category management for work orders in Atlas CMMS.

    Operations:
      Read: list_additional_costs, list_labor, list_cost_categories, list_time_categories
      Write: create_additional_cost, update_additional_cost, create_labor, update_labor
      Delete: delete_additional_cost, delete_labor

    Args:
        operation: One of the operations listed above.
        pk: Cost or labor entry ID.
        data: Dict of fields for create/update.
        wo_id: Work order ID (required for list_additional_costs/list_labor).

    Returns:
        JSON string with cost/labor data or {"error": "..."}.
    """
    c = await get_client()

    if operation == "list_additional_costs":
        return _json(await c.cost_list(wo_id))
    elif operation == "create_additional_cost":
        return _json(await c.cost_create(data or {}))
    elif operation == "update_additional_cost":
        return _json(await c.cost_update(pk, data or {}))
    elif operation == "delete_additional_cost":
        return _json(await c.cost_delete(pk))
    elif operation == "list_labor":
        return _json(await c.labor_list(wo_id))
    elif operation == "create_labor":
        return _json(await c.labor_create(data or {}))
    elif operation == "update_labor":
        return _json(await c.labor_update(pk, data or {}))
    elif operation == "delete_labor":
        return _json(await c.labor_delete(pk))
    elif operation == "list_cost_categories":
        return _json(await c.cost_list_categories())
    elif operation == "list_time_categories":
        return _json(await c.time_list_categories())
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 13: Analytics (20 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS Analytics & Reporting",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
@_safe("analytics")
async def analytics(
    operation: str,
    asset_id: int = None,
    start: str = None,
    end: str = None,
) -> str:
    """Analytics and KPI reporting for Atlas CMMS. All operations are read-only.

    Operations:
      WO: wo_complete_overview, wo_incomplete_overview, wo_priority,
        wo_statuses, wo_hours, wo_counts_by_user, wo_counts_by_priority,
        wo_costs_time, wo_costs_date
      Asset: asset_time_cost, asset_overview, asset_downtimes, asset_mtbf,
        asset_meantimes, asset_repair_times, asset_downtime_costs,
        asset_single_overview
      Other: part_consumptions, request_overview, user_overview

    Args:
        operation: One of the operations listed above.
        asset_id: Asset ID for asset_single_overview.
        start: Start date (ISO format, e.g. "2024-01-01").
        end: End date (ISO format, e.g. "2024-12-31").

    Returns:
        JSON string with analytics data.
    """
    c = await get_client()
    date_kw = {}
    if start:
        date_kw["start"] = start
    if end:
        date_kw["end"] = end

    # WO Analytics
    if operation == "wo_complete_overview":
        return _json(await c.analytics_wo_complete_overview(**date_kw))
    elif operation == "wo_incomplete_overview":
        return _json(await c.analytics_wo_incomplete_overview(**date_kw))
    elif operation == "wo_priority":
        return _json(await c.analytics_wo_priority(**date_kw))
    elif operation == "wo_statuses":
        return _json(await c.analytics_wo_statuses(**date_kw))
    elif operation == "wo_hours":
        return _json(await c.analytics_wo_hours(**date_kw))
    elif operation == "wo_counts_by_user":
        return _json(await c.analytics_wo_counts_by_user(**date_kw))
    elif operation == "wo_counts_by_priority":
        return _json(await c.analytics_wo_counts_by_priority(**date_kw))
    elif operation == "wo_costs_time":
        return _json(await c.analytics_wo_costs_time(**date_kw))
    elif operation == "wo_costs_date":
        return _json(await c.analytics_wo_costs_date(**date_kw))
    # Asset Analytics
    elif operation == "asset_time_cost":
        return _json(await c.analytics_asset_time_cost(**date_kw))
    elif operation == "asset_overview":
        return _json(await c.analytics_asset_overview(**date_kw))
    elif operation == "asset_downtimes":
        return _json(await c.analytics_asset_downtimes(**date_kw))
    elif operation == "asset_mtbf":
        return _json(await c.analytics_asset_mtbf(**date_kw))
    elif operation == "asset_meantimes":
        return _json(await c.analytics_asset_meantimes(**date_kw))
    elif operation == "asset_repair_times":
        return _json(await c.analytics_asset_repair_times(**date_kw))
    elif operation == "asset_downtime_costs":
        return _json(await c.analytics_asset_downtime_costs(**date_kw))
    elif operation == "asset_single_overview":
        return _json(await c.analytics_asset_single_overview(asset_id, **date_kw))
    # Part, Request, User Analytics
    elif operation == "part_consumptions":
        return _json(await c.analytics_part_consumptions(**date_kw))
    elif operation == "request_overview":
        return _json(await c.analytics_request_overview(**date_kw))
    elif operation == "user_overview":
        return _json(await c.analytics_user_overview())
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 14: File (11 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS File & Import/Export",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("file")
async def file(
    operation: str,
    pk: int = None,
    data: dict = None,
    file_path: str = None,
    folder: str = None,
    hidden: bool = False,
    file_type: str = "OTHER",
    task_id: int = None,
    entity_type: str = None,
    entity_id: int = None,
    import_data: list = None,
    filter_fields: list = None,
    page_num: int = 0,
    page_size: int = 25,
    sort_field: str = None,
    direction: str = "DESC",
) -> str:
    """File management, import, and export operations in Atlas CMMS.

    Operations:
      Read: search, get, export_work_orders, export_assets, export_parts
      Write: upload, update, import_work_orders, import_assets, import_parts
      Delete: delete

    Args:
        operation: One of the operations listed above.
        pk: File ID for get/update/delete.
        data: Dict of fields for update.
        file_path: Local file path for upload.
        folder: Storage folder/path for upload (required by backend). Defaults to "mcp-test".
        hidden: Whether the uploaded file is hidden (default False).
        file_type: File type enum ("IMAGE" or "OTHER"). Defaults to "OTHER".
        task_id: Optional task ID to associate during upload.
        entity_type/entity_id: Optional convenience to attach uploaded file(s) to another entity.
          Supported: entity_type="WORK_ORDER" will call wo_add_files(entity_id, file_ids).
        import_data: List of records for import operations.
        filter_fields/page_num/page_size/sort_field/direction: Search criteria.

    Returns:
        JSON string with file/export/import data or {"error": "..."}.
    """
    c = await get_client()
    kw = _search_kwargs(filter_fields, page_num, page_size, sort_field, direction)

    if operation == "search":
        return _json(await c.file_search(**kw))
    elif operation == "get":
        return _json(await c.file_get(pk))
    elif operation == "upload":
        folder_eff = folder or "mcp-test"
        uploaded = await c.file_upload(
            file_path, folder=folder_eff, hidden=hidden, file_type=file_type, task_id=task_id
        )
        # Optionally attach uploaded file(s) to a Work Order.
        if entity_type and entity_id and isinstance(uploaded, list):
            file_ids = [f.get("id") for f in uploaded if isinstance(f, dict) and f.get("id")]
            if file_ids and entity_type.upper() == "WORK_ORDER":
                await c.wo_add_files(entity_id, file_ids)
        return _json(uploaded)
    elif operation == "update":
        return _json(await c.file_update(pk, data or {}))
    elif operation == "delete":
        return _json(await c.file_delete(pk))
    elif operation == "export_work_orders":
        return _json(await c.export_work_orders())
    elif operation == "export_assets":
        return _json(await c.export_assets())
    elif operation == "export_parts":
        return _json(await c.export_parts())
    elif operation == "import_work_orders":
        return _json(await c.import_work_orders(import_data or []))
    elif operation == "import_assets":
        return _json(await c.import_assets(import_data or []))
    elif operation == "import_parts":
        return _json(await c.import_parts(import_data or []))
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Tool 15: System (15 operations)
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Atlas CMMS System & Configuration",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("system")
async def system(
    operation: str,
    pk: int = None,
    data: dict = None,
) -> str:
    """System administration, auth, company settings, and configuration in Atlas CMMS.

    Operations:
      Read: health_check, get_me, get_company, get_settings, get_preferences,
        list_currencies, get_custom_field, list_relations, list_workflows
      Write: refresh_token, update_company, update_preferences,
        create_custom_field, update_field_config, create_relation

    Args:
        operation: One of the operations listed above.
        pk: ID for company/settings/preferences/custom_field/field_config operations.
        data: Dict of fields for update/create operations.

    Returns:
        JSON string with system data or {"error": "..."}.
    """
    c = await get_client()

    if operation == "health_check":
        return _json(await c.health_check())
    elif operation == "get_me":
        return _json(await c.get_me())
    elif operation == "refresh_token":
        return _json(await c.refresh_token())
    elif operation == "get_company":
        return _json(await c.get_company(pk))
    elif operation == "update_company":
        return _json(await c.update_company(pk, data or {}))
    elif operation == "get_settings":
        return _json(await c.get_settings(pk))
    elif operation == "get_preferences":
        return _json(await c.get_preferences())
    elif operation == "update_preferences":
        return _json(await c.update_preferences(pk, data or {}))
    elif operation == "list_currencies":
        return _json(await c.list_currencies())
    elif operation == "get_custom_field":
        return _json(await c.get_custom_field(pk))
    elif operation == "create_custom_field":
        return _json(await c.create_custom_field(data or {}))
    elif operation == "update_field_config":
        return _json(await c.update_field_config(pk, data or {}))
    elif operation == "list_relations":
        return _json(await c.list_relations())
    elif operation == "create_relation":
        return _json(await c.create_relation(data or {}))
    elif operation == "list_workflows":
        return _json(await c.list_workflows())
    else:
        return _json({"error": f"Unknown operation: {operation}"})


# ═══════════════════════════════════════════════════════════════════════
# Entry point — dual transport
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"

    if transport == "sse":
        port = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("PORT", "3075"))
        logger.info(f"Starting Atlas CMMS MCP Server on SSE port {port}")
        mcp.run(transport="sse", port=port)
    else:
        logger.info("Starting Atlas CMMS MCP Server on STDIO")
        mcp.run(transport="stdio")
