"""Pydantic response models for OpenAPI schema generation."""

from pydantic import BaseModel, Field


# --- Sites ---

class SiteItem(BaseModel):
    site_id: str = Field(examples=["RWS01_MONIBAS_0161hrr0346ra"])
    name: str | None = Field(None, examples=["A2 hmp 34.6 Re"])
    road: str | None = Field(None, examples=["A2"])
    lanes: int | None = Field(None, examples=[3])
    equipment: str | None = Field(None, examples=["loop"])
    direction: str | None = Field(None, examples=["positive"])
    lat: float = Field(examples=[52.0836])
    lon: float = Field(examples=[5.1251])


class SiteListResponse(BaseModel):
    total_count: int = Field(examples=[142])
    limit: int = Field(examples=[100])
    offset: int = Field(examples=[0])
    data: list[SiteItem]


# --- Speeds ---

class SpeedItem(BaseModel):
    site_id: str = Field(examples=["RWS01_MONIBAS_0161hrr0346ra"])
    name: str | None = Field(None, examples=["A2 hmp 34.6 Re"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    speed_kmh: float | None = Field(None, examples=[98.3])
    flow_veh_hr: int | None = Field(None, examples=[4200])
    road: str | None = Field(None, examples=["A2"])
    lat: float = Field(examples=[52.0836])
    lon: float = Field(examples=[5.1251])


class SpeedListResponse(BaseModel):
    total_count: int = Field(examples=[142])
    limit: int = Field(examples=[100])
    offset: int = Field(examples=[0])
    data: list[SpeedItem]


class LaneDetail(BaseModel):
    lane: int = Field(examples=[1])
    speed_kmh: float | None = Field(None, examples=[102.5])
    flow_veh_hr: int | None = Field(None, examples=[1400])


class SpeedSiteLanes(BaseModel):
    site_id: str = Field(examples=["RWS01_MONIBAS_0161hrr0346ra"])
    name: str | None = Field(None, examples=["A2 hmp 34.6 Re"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    lanes: list[LaneDetail]


class SpeedSiteAggregate(BaseModel):
    site_id: str = Field(examples=["RWS01_MONIBAS_0161hrr0346ra"])
    name: str | None = Field(None, examples=["A2 hmp 34.6 Re"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    speed_kmh: float | None = Field(None, examples=[98.3])
    flow_veh_hr: int | None = Field(None, examples=[4200])


class SpeedHistoryPoint(BaseModel):
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    lane: int = Field(examples=[1])
    speed_kmh: float | None = Field(None, examples=[98.3])
    flow_veh_hr: int | None = Field(None, examples=[4200])


class SpeedHistoryResponse(BaseModel):
    site_id: str = Field(examples=["RWS01_MONIBAS_0161hrr0346ra"])
    resolution: str = Field(examples=["5m"])
    count: int = Field(examples=[60])
    data: list[SpeedHistoryPoint]


# --- Journey Times ---

class JourneyTimeItem(BaseModel):
    site_id: str = Field(examples=["PGL03_FV_P1a"])
    name: str | None = Field(None, examples=["A28 Amersfoort-Harderwijk"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    duration_sec: float | None = Field(None, examples=[245.0])
    ref_duration_sec: float | None = Field(None, examples=[180.0])
    delay_sec: float | None = Field(None, examples=[65.0])
    delay_ratio: float | None = Field(None, examples=[1.361])
    accuracy: float | None = Field(None, examples=[0.95])
    quality: float | None = Field(None, examples=[85.0])
    input_values: int | None = Field(None, examples=[42])
    road: str | None = Field(None, examples=["A28"])
    lat: float | None = Field(None, examples=[52.1543])
    lon: float | None = Field(None, examples=[5.3872])


class JourneyTimeListResponse(BaseModel):
    total_count: int = Field(examples=[38])
    limit: int = Field(examples=[100])
    offset: int = Field(examples=[0])
    data: list[JourneyTimeItem]


class JourneyTimeDetail(BaseModel):
    site_id: str = Field(examples=["PGL03_FV_P1a"])
    name: str | None = Field(None, examples=["A28 Amersfoort-Harderwijk"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    duration_sec: float | None = Field(None, examples=[245.0])
    ref_duration_sec: float | None = Field(None, examples=[180.0])
    delay_sec: float | None = Field(None, examples=[65.0])
    delay_ratio: float | None = Field(None, examples=[1.361])
    accuracy: float | None = Field(None, examples=[0.95])
    quality: float | None = Field(None, examples=[85.0])
    input_values: int | None = Field(None, examples=[42])


class JourneyTimeHistoryPoint(BaseModel):
    site_id: str = Field(examples=["PGL03_FV_P1a"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    duration_sec: float | None = Field(None, examples=[245.0])
    ref_duration_sec: float | None = Field(None, examples=[180.0])
    delay_sec: float | None = Field(None, examples=[65.0])
    delay_ratio: float | None = Field(None, examples=[1.361])
    quality: float | None = Field(None, examples=[85.0])


class JourneyTimeHistoryResponse(BaseModel):
    site_id: str = Field(examples=["PGL03_FV_P1a"])
    resolution: str = Field(examples=["5m"])
    count: int = Field(examples=[60])
    data: list[JourneyTimeHistoryPoint]


class CongestionItem(BaseModel):
    site_id: str = Field(examples=["PGL03_FV_P1a"])
    name: str | None = Field(None, examples=["A28 Amersfoort-Harderwijk"])
    timestamp: str = Field(examples=["2026-02-17T12:00:00+00:00"])
    duration_sec: float | None = Field(None, examples=[360.0])
    ref_duration_sec: float | None = Field(None, examples=[180.0])
    delay_sec: float | None = Field(None, examples=[180.0])
    delay_ratio: float | None = Field(None, examples=[2.0])
    quality: float | None = Field(None, examples=[85.0])
    road: str | None = Field(None, examples=["A28"])
    lat: float | None = Field(None, examples=[52.1543])
    lon: float | None = Field(None, examples=[5.3872])


class CongestionResponse(BaseModel):
    total_count: int = Field(examples=[12])
    threshold: float = Field(examples=[1.5])
    data: list[CongestionItem]


# --- Health ---

class HealthResponse(BaseModel):
    status: str = Field(examples=["healthy"])
    database: str = Field(examples=["connected"])
    redis: str = Field(examples=["connected"])
    last_speed_update: str | None = Field(None, examples=["2026-02-17T12:00:00+00:00"])
    last_jt_update: str | None = Field(None, examples=["2026-02-17T12:00:00+00:00"])
    checked_at: str = Field(examples=["2026-02-17T12:00:01+00:00"])
