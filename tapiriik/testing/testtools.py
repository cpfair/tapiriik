from tapiriik.services import Service
from tapiriik.services.interchange import Activity, ActivityType

import random


class MockServiceA:
    ID = "mockA"
    SupportedActivities = [ActivityType.Rowing]


class MockServiceB:
    ID = "mockB"
    SupportedActivities = [ActivityType.Rowing, ActivityType.Wheelchair]


class TestTools:
    def create_mock_svc_record(svc):
        return {"Service": svc.ID}

    def create_mock_upload_record(svc):
        return {"ActivityID": random.randint(1, 1000), "Connection": TestTools.create_mock_svc_record(svc)}

    def create_mock_services():
        mockA = MockServiceA()
        mockB = MockServiceB()
        Service._serviceMappings["mockA"] = mockA
        Service._serviceMappings["mockB"] = mockB
        return (mockA, mockB)