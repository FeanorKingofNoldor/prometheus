from apatheon.core.database import get_db_manager
from fastapi import APIRouter

from prometheus.event_desk.storage import Store

router = APIRouter(prefix="/api/event-desk", tags=["event-desk"])


@router.get("")
def event_desk():
    return Store(get_db_manager()).dashboard()
