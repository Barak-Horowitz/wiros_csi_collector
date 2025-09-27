"""
This script subscribes to the ROS Wifi Topic, once a message is received on this topic
we write the csi data to a local binary file and save the message metadata in a dict.
once a certain threshold has been reached (number of messages received, time passed, size of file, etc. we haven't decided yet)
We then post this binary file and metadata to our ingestion server so it can place it in the S3 server and DynamoDB respectively 
"""

import json
from queue import Queue, Empty
import threading
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy
import time, os, requests, struct 
from rf_msgs.msg import Wifi # type: ignore - Compilation warning was incorrect
from _CONST import _Const
import copy 



class CSICollector(Node):
    def __init__(self):
        super().__init__('csi_collector')
        
        # Initialize constants
        self.CONST = _Const(os)
        
        # Initialize global variables
        self.error_flag = False # set to true if we encounter errors on the message upload process
        self.messages_received = 0 # stores the number of messages we have received since service has started 
        self.file_name = str(time.time()) + self.CONST.DEVICE_NAME # file names must be unique and include a device identifier in order to be stored/managed by the S3 server
        self.bytes_written_to_file = 0 # stores the amount of bytes we have written to the binary file (used to add offset in file into metadata)
        self.CSI_file_metadata = [] # stores the metadata of the messages so we can send them to the ingestion server
        self.file_writer = None # used to keep file open in between writes increasing i/o throughput 
        
        # Create QoS profile to match the publisher (best_effort)
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
        
        # the ingestion queue and worker handle sending post requests to our ingestion server
        self.ingestion_queue = Queue(maxsize=100)
        self.stop_event = threading.Event()
        self.ingestion_thread = threading.Thread(target=self.ingestion_worker_loop, daemon=True)
        self.ingestion_thread.start()
    
    # helper function - used to convert mac addresses to string to place in metadata dict
    def convert_mac_address_to_string(self, mactup):
        assert len(mactup) == 6, f"Invalid MAC tuple: {mactup}"
        return "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}".format(mactup[0], mactup[1], mactup[2], mactup[3], mactup[4], mactup[5])
    
    # helper function, given a collection of real and imaginary CSI floats convert to binary and write it to the current file
    # returns number of bytes successfully written to file 
    def write_file(self, real_list, imag_list):
        if self.file_writer is None:
            # Ensure directory exists
            os.makedirs('binary_data', exist_ok=True)
            self.file_writer = open(f'binary_data/{self.file_name}', 'ab')
            
        # Combine real and imaginary parts 
        csi_data_array = real_list + imag_list 
        binary_csi_data = struct.pack('d' * len(csi_data_array), *csi_data_array)
        return self.file_writer.write(binary_csi_data)
    
    # helper function closes file writer once we have finished writing to file
    def close_file_writer(self):
        if(self.file_writer is not None):
            self.file_writer.close()
        self.file_writer = None
        
    # once we have received a message we parse its metadata and add
    # it to the metadata array, we then write its contents to the binary file 
    # we have open if the publishing threshold has been reached, we then 
    # publish it to the ingestion server and restart the process
    def callback(self, msg):
        if self.error_flag:
            print("error detected during data collection")
        
        # write data to file and update globals
        bytes_written_before_upload = self.bytes_written_to_file
        self.bytes_written_to_file += self.write_file(msg.csi_real,msg.csi_imag)
        self.messages_received += 1
        message_size = self.bytes_written_to_file - bytes_written_before_upload
        # grab message metadata and place it in file metadata array 
        message_metadata = {
            "mac_address" : self.convert_mac_address_to_string(msg.txmac),
            "timestamp" : msg.header.stamp.sec + msg.header.stamp.nanosec/(10**9),
            "device_name" : self.CONST.DEVICE_NAME,
            "offset_in_file" : bytes_written_before_upload,
            "message_size" : message_size,
            "message_id": msg.msg_id,
            "access_point" : msg.ap_id,
            "channel_number" : msg.chan,
            "matrix_rows" : msg.n_rows,
            "matrix_columns" : msg.n_cols,
            "bandwidth" : msg.bw,
            "spatial_channels" : msg.mcs,
            "rssi" : msg.rssi,
            "fc" : msg.fc,
            "sequence_number" : msg.seq_num   
        }
        self.CSI_file_metadata.append(message_metadata)
        
        # if our file is full then reset variables and have a background thread send it to the ingestion server to process
        if self.messages_received  % self.CONST.NUMBER_OF_MESSAGES_PER_FILE == 0:
            file_path = os.path.abspath(f'binary_data/{self.file_name}')
            try: # need to pass in copies so background thread isn't affected by changes in main thread
                self.ingestion_queue.put_nowait((copy.deepcopy(self.CSI_file_metadata), copy.deepcopy(file_path), 
                                            self.CONST.SAVE_TO_INGESTION_SERVER, self.CONST.SAVE_TO_S3_STORAGE))
            except Exception:
                print("ERROR: ingestion queue is full - this should never happen")
            #reset bytes written to file, metadata array, and close file writer
            self.bytes_written_to_file = 0
            self.CSI_file_metadata = []  # Clear metadata for next batch
            self.close_file_writer()
            #rotate file name for next file
            self.file_name = str(time.time()) + self.CONST.DEVICE_NAME
            
            
    # ----------------- ingestion worker -----------------
    # background thread responsible for posting CSI data + 
    # metadata to server that handles placing it in S3 and dynamoDB
    def ingestion_worker_loop(self):
        while not self.stop_event.is_set():
            try:
                job = self.ingestion_queue.get(timeout=1.0)
            except Empty:
                continue

            metadata, path, save_to_local_server, save_to_s3_storage = job
            if not os.path.exists(path):
                print(f'ingestion worker: file not found {path}, skipping')
                self.ingestion_queue.task_done()
                continue

            url = self.CONST.UPLOAD_ENDPOINT
            if url is None:
                print('INGESTION_URL not set: skipping ingestion POST')
                self.ingestion_queue.task_done()
                continue

            # attempt to post to server, 
            # if post fails and server tells us its because our data is malformed immediately stop
            # if post fails and server tells us its because of an issue on their end try again a maximum of 3 times
            # waiting longer in between each one so server has time to deal with congestion 
            max_attempts = 3
            backoff = 1.0
            success = False
            for attempt in range(max_attempts):
                try:
                    with open(path, 'rb') as fh:
                        # send metadata as a single form field (JSON array) and the CSI blob as a file
                        data = {
                            'metadata': json.dumps(metadata), # json array 
                            'save_to_server' : save_to_local_server, # bool val
                            'save_to_s3_storage' : save_to_s3_storage # bool val
                        }
                        files = {
                            'csi_blob': (os.path.basename(path), fh, 'application/octet-stream')
                        }
                        resp = requests.post(url, data=data, files=files, timeout=10)
                    if resp.status_code >= 200 and resp.status_code < 300:
                        print(f'ingestion POST succeeded for {path}: {resp.status_code}')
                        success = True
                        break
                    elif resp.status_code >= 400 and resp.status_code < 500:
                        print(f'ingestion POST failed due to malformed data {resp.status_code}: {resp.text}')
                        success = False
                        break
                    else:
                        print(f'ingestion POST failed due to overload on server - waiting and then trying again {resp.status_code}: {resp.text}')
                
                except Exception as e:
                    print(f'ingestion POST exception (attempt {attempt}): {e}')

                time.sleep(backoff)
                backoff *= 2

            if success:
                # remove local file on success unless LOCAL_COPY is True
                if not self.CONST.LOCAL_COPY:
                    try:
                        os.remove(path)
                        print(f'removed local file after ingestion: {path}')
                    except Exception as e:
                        print(f'failed to remove local file {path}: {e}')
            else:
                print(f'failed to ingest {path} after retries; leaving on disk')

            self.ingestion_queue.task_done()

    def shutdown(self):
        """Gracefully shutdown the collector"""
        print("Starting graceful shutdown...")
        
        # 1. Close current file if open
        if self.file_writer is not None:
            try:
                self.file_writer.flush()  # Ensure all data is written
                self.file_writer.close()
                print(f"Closed current file: {self.file_name}")
            except Exception as e:
                print(f"Error closing file: {e}")
            self.file_writer = None
        
        # 2. Signal background thread to stop
        self.stop_event.set()
        
        # 3. Process any remaining items in queue (with timeout)
        remaining_items = 0
        while not self.ingestion_queue.empty():
            try:
                remaining_items += 1
                if remaining_items > 10:  # Prevent infinite wait
                    print(f"Warning: {self.ingestion_queue.qsize()} items still in queue, forcing shutdown")
                    break
                time.sleep(0.1)  # Give worker time to process
            except:
                break
        
        # 4. Wait for background thread to finish (with timeout)
        if self.ingestion_thread.is_alive():
            print("Waiting for ingestion worker to finish...")
            self.ingestion_thread.join(timeout=5.0)  # 5 second timeout
            if self.ingestion_thread.is_alive():
                print("Warning: Ingestion thread did not shutdown cleanly")
        
        # 5. Save any partial data if we have messages
        if self.CSI_file_metadata and self.messages_received > 0:
            try:
                file_path = os.path.abspath(f'binary_data/{self.file_name}')
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    print(f"Saving partial batch with {len(self.CSI_file_metadata)} messages")
                    self.ingestion_queue.put_nowait((
                        copy.deepcopy(self.CSI_file_metadata), 
                        file_path,
                        self.CONST.SAVE_TO_INGESTION_SERVER, 
                        self.CONST.SAVE_TO_S3_STORAGE
                    ))
                    # Give one last chance to process
                    time.sleep(1.0)
            except Exception as e:
                print(f"Error saving partial data: {e}")
        
        print("Graceful shutdown complete")

    
# TODO: For now user MUST DO FOLLOWING
# 1) cd ~/wifi-deployment && ansible-playbook -i inventory.ini -K pb_start_collection.yml (password is robot123!) - if this fails run sudo reboot now
# 2) source ~/wifi_ws/install/setup.bash command before starting the collector
# 3) python3 collector.py combine steps into a bash startup script
def main():
    
    print("starting CSI data collection service")
    
    # Initialize ROS 2
    rclpy.init()
    
    # Create and run the node
    csi_collector = CSICollector()
    
    try:
        rclpy.spin(csi_collector)
    except KeyboardInterrupt:
        print("\nReceived shutdown signal (Ctrl+C)")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Graceful shutdown sequence
        try:
            csi_collector.shutdown()
        except Exception as e:
            print(f"Error during shutdown: {e}")
        
        # Clean up ROS
        try:
            csi_collector.destroy_node()
        except Exception as e:
            print(f"Error destroying node: {e}")
            
        # Only shutdown if ROS context is still active
        try:
            if rclpy.ok():  # Check if ROS context is still running
                rclpy.shutdown()
        except Exception as e:
            # Ignore double shutdown errors
            if "rcl_shutdown already called" not in str(e):
                print(f"Error shutting down RclPy: {e}")
            
        print("CSI collector stopped")

if __name__ == "__main__":
    main()
    
    