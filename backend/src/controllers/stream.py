from src.services import stream as service
from src.schemas.entities import IngestInput


def list_items():
    return service.list_streams()


def detail(station):
    return service.detail(station)


def notifications(station):
    return service.notifications(station)


def notification(station, notification_id):
    return service.notification(station, notification_id)


def ingest(data: IngestInput):
    return service.ingest(data)


def preview(data: IngestInput):
    return service.preview(data)
