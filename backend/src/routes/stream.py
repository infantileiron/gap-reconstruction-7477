from fastapi import APIRouter, Depends

from src.controllers import stream as controller
from src.schemas.entities import IngestInput
from src.services.auth import current, editor

router = APIRouter(prefix="/streams", tags=["streams"])


@router.get("")
def list_items(user=Depends(current)):
    return controller.list_items()


@router.post("/ingest", status_code=201)
def ingest(data: IngestInput, user=Depends(editor)):
    return controller.ingest(data)


@router.post("/preview")
def preview(data: IngestInput, user=Depends(editor)):
    return controller.preview(data)


@router.get("/{station}")
def detail(station: str, user=Depends(current)):
    return controller.detail(station)


@router.get("/{station}/notifications")
def notifications(station: str, user=Depends(current)):
    return controller.notifications(station)


@router.get("/{station}/notifications/{notification_id}")
def notification(station: str, notification_id: str, user=Depends(current)):
    return controller.notification(station, notification_id)
