# Atlas CMMS MCP Server

MCP server for [Atlas CMMS](https://www.atlascmms.com) (Grashjs) maintenance management. Provides 15 parameterized tools covering 194 operations for work orders, assets, preventive maintenance, meters, parts, purchase orders, locations, vendors, teams, tasks, requests, costs, analytics, files, and system administration.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- An Atlas CMMS (Grashjs) instance
- Admin user credentials (email/password)

## Setup

```bash
# Clone the repository
git clone https://github.com/puran-water/atlas-cmms-mcp.git
cd atlas-cmms-mcp

# Copy the example environment file and fill in your values
cp .env.example .env

# Install dependencies
uv sync
```

Edit `.env` with your Atlas CMMS instance URL and credentials:

```
ATLAS_URL=https://your-atlas-cmms-instance.example.com
ATLAS_EMAIL=admin@example.com
ATLAS_PASSWORD=your-password-here
```

## Usage

### STDIO mode (default)

```bash
uv run python server.py
```

### SSE mode (HTTP transport)

```bash
uv run python server.py sse --port 3075
```

### Claude Desktop / MCP client configuration

Add to your MCP client config (e.g. `~/.claude/mcp.json`):

```json
{
  "mcpServers": {
    "atlas-cmms-mcp": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/atlas-cmms-mcp", "python", "server.py"],
      "env": {
        "ATLAS_URL": "https://your-atlas-cmms-instance.example.com",
        "ATLAS_EMAIL": "admin@example.com",
        "ATLAS_PASSWORD": "your-password-here"
      }
    }
  }
}
```

Or for SSE transport:

```json
{
  "mcpServers": {
    "atlas-cmms-mcp": {
      "url": "http://localhost:3075/sse"
    }
  }
}
```

## Tools

Each tool uses a parameterized `operation` field to select the specific action.

| Tool | Operations | Description |
|------|-----------|-------------|
| `work_order` | 20 | Work order lifecycle (search, create, status changes, history, categories) |
| `asset` | 18 | Asset hierarchy, tracking, downtimes, NFC/barcode lookup |
| `preventive_maintenance` | 14 | PM schedules, meter-based triggers |
| `cmms_part` | 15 | Parts inventory, quantities, multi-part assemblies |
| `cmms_purchase_order` | 8 | Purchase order management with categories |
| `meter` | 10 | Meters, readings, condition monitoring |
| `location` | 11 | Location hierarchy with floor plans |
| `vendor` | 10 | Vendor & customer management |
| `team` | 12 | Team, user, and role management |
| `task` | 12 | Work order tasks and reusable checklists |
| `request` | 8 | Work order requests with approval workflow |
| `cost` | 10 | Labor tracking and additional cost management |
| `analytics` | 20 | KPIs: MTBF, MTTR, WO statistics, asset performance, costs |
| `file` | 11 | File upload/download, import/export (WOs, assets, parts) |
| `system` | 15 | Auth, company settings, preferences, currencies, custom fields |

## API Notes

- **Authentication**: JWT-based. The server logs in automatically using the provided credentials and refreshes the token on 401 responses.
- **Search pattern**: All list endpoints use `POST /search` with a `SearchCriteria` body containing `filterFields`, `pageNum`, `pageSize`, `sortField`, and `direction`.
- **License-gated operations**: Some operations (vendor/customer create, labor/cost tracking, asset downtimes, file uploads, WO linking) require an active Atlas CMMS license. These return 403 without a valid license key configured on the backend.

## Architecture

- **`server.py`** — FastMCP server with 15 parameterized tools and dual transport (STDIO/SSE)
- **`client.py`** — Async HTTP client using aiohttp with JWT authentication, auto-refresh on 401, and the standard Grashjs search pattern

## License

MIT
