""" stores constants used in collection script """

class _Const:
    def __init__(self, os):
        pass
    # TODO: add reading of AWS keys + buckets
    
    DEVICE_NAME = "wiros_pi_node_1"
    ROS_TOPIC = "/csi"
    HEALTH_ENDPOINT = "http://137.110.198.43:8000/health"
    UPLOAD_ENDPOINT = "http://137.110.198.43:8000/ingest"
    NUMBER_OF_MESSAGES_PER_FILE = 500
    SAVE_TO_INGESTION_SERVER = True
    SAVE_TO_S3_STORAGE = False
    LOCAL_COPY = False