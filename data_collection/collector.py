"""
This script subscribes to the ROS Wifi Topic, once a message is received on this topic
we write the csi data to a local binary file and save the message metadata in a dict.
once a certain threshold has been reached (number of messages received, time passed, size of file, etc. we haven't decided yet)
We then post this binary file and metadata to our ingestion server so it can place it in the S3 server and DynamoDB respectively 
"""

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy
import time, os
from rf_msgs.msg import Wifi # type: ignore - Compilation warning was incorrect
from _CONST import _Const



class CSICollector(Node):
    def __init__(self):
        super().__init__('csi_collector')
        
        # Initialize constants
        self.CONST = _Const(os)
        
        # Initialize global variables
        self.error_flag = False # set to true if we encounter errors on the message upload process
        self.count = 0 # stores the number of messages we have received since service has started 
        self.file_name = str(time.time()) + self.CONST.DEVICE_NAME # file names must be unique and include a device identifier in order to be stored/managed by the S3 server
        self.CSI_file_metadata = []
        
        # Create QoS profile to match the publisher (reliable)
        qos_profile = QoSProfile(
            depth=10,
            reliability=ReliabilityPolicy.RELIABLE,  # Match the publisher
            durability=DurabilityPolicy.VOLATILE
        )
        
        # Create subscription - currently holding up to 10 messages in buffer before dropping
        self.subscription = self.create_subscription(
            Wifi,
            self.CONST.ROS_TOPIC,
            self.callback,
            qos_profile
        )
        self.subscription  # prevent unused variable warning
        
    
    # once we have received a message we parse its metadata and add
    # it to the metadata array, we then write its contents to the binary file 
    # we have open if the publishing threshold has been reached, we then 
    # publish it to the ingestion server and restart the process
    def callback(self, msg):
        print("received CSI Data")
        self.get_logger().info(f'Received CSI data: {msg}')
        # TODO: Implement CSI data processing and file writing logic
    
    
# TODO: For now user MUST run
# source ~/wifi_ws/install/setup.bash command before starting the collector - combine both into a bash startup script
def main():
    print("starting CSI data collection service")
    
    # Initialize ROS 2
    rclpy.init()
    
    # Create and run the node
    csi_collector = CSICollector()
    
    try:
        rclpy.spin(csi_collector)
    except KeyboardInterrupt:
        print("Shutting down CSI collector...")
    finally:
        csi_collector.destroy_node()
        rclpy.shutdown()

if __name__ == "__main__":
    main()
    
    