"""GET /sites and GET /sites/{site_id} — sensor discovery."""

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(prefix="/sites", tags=["sites"])


@router.get("", summary="List sites", description="""
Search and filter the 99,324 traffic measurement sites across the Netherlands.
Each site has a unique ID, road name, coordinates, lane count, and equipment type.

Without filters, returns all sites (paginated). Use `bbox` for geographic queries
or `road` to find all sensors on a specific highway.

**Example:** `GET /sites?road=A28&limit=10`
""")
async def list_sites(
    request: Request,
    bbox: str | None = Query(None, description="Bounding box filter: lat1,lon1,lat2,lon2 (e.g. 52.3,4.8,52.4,5.0)"),
    road: str | None = Query(None, description="Road name (e.g. A28, N201, A2)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip (for pagination)"),
):
    pool = request.app.state.pool

    conditions = []
    params = []
    idx = 1

    if bbox:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(400, "bbox must be lat1,lon1,lat2,lon2")
        lat1, lon1, lat2, lon2 = parts
        conditions.append(
            f"ST_Within(geom, ST_MakeEnvelope(${idx}, ${idx+1}, ${idx+2}, ${idx+3}, 4326))"
        )
        params.extend([lon1, lat1, lon2, lat2])
        idx += 4

    if road:
        conditions.append(f"road = ${idx}")
        params.append(road)
        idx += 1

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM sites {where}", *params
        )
        rows = await conn.fetch(
            f"""
            SELECT site_id, name, road, lanes, equipment, direction,
                   ST_Y(geom) AS lat, ST_X(geom) AS lon
            FROM sites {where}
            ORDER BY site_id
            LIMIT ${idx} OFFSET ${idx+1}
            """,
            *params, limit, offset,
        )

    return {
        "total_count": total,
        "limit": limit,
        "offset": offset,
        "data": [dict(r) for r in rows],
    }


@router.get("/{site_id}", summary="Get site details", description="""
Get full details for a single measurement site by its ID.

**Example:** `GET /sites/RWS01_MONIBAS_0161hrr0346ra`
""")
async def get_site(request: Request, site_id: str):
    pool = request.app.state.pool

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT site_id, name, road, lanes, equipment, direction,
                   ST_Y(geom) AS lat, ST_X(geom) AS lon
            FROM sites WHERE site_id = $1
            """,
            site_id,
        )

    if row is None:
        raise HTTPException(404, f"Site {site_id} not found")

    return dict(row)
