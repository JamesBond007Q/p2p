import os
import struct
from time import sleep
from threading import Thread, Lock
from abc import ABC, abstractmethod

from protocol import Protocol


class AbstractP2PClient(ABC):
    CHUNK_SIZE = 1024
    ACK_ARRIVAL_TIME = 0.5
    WAIT_BEFORE_FILE_STREAM_RELEASE = 4

    FILE_TRANSFER_STREAMS = [7771, 7772, 7773, 7774]

    def __init__(self, peer):
        self._peer = peer
        self._next_message_id = 0
        self._message_id_to_was_acked = {} # Check if reliable messages were acked or not
        self._messages_ids_that_have_been_received = {} # In order to avoid executing the same command twice due to retransmit
        self._file_stream_id_to_chunks = {stream_id:{} for stream_id in self.FILE_TRANSFER_STREAMS} # This is for RECEIVING on a file stream. Every chunk is saved in a dictionary when the key the chunk's index.
        self._file_stream_id_to_is_available = {stream_id:True for stream_id in self.FILE_TRANSFER_STREAMS} # This is for SENDING on a file stream
        self._file_streams_lock = Lock()
        Thread(target=self._listen).start()

    @abstractmethod
    def on_reliable_message(self, message):
        pass

    @abstractmethod
    def on_unreliable_message(self, message):
        pass

    @abstractmethod
    def on_reliable_stream_message(self, message, stream_id):
        pass

    @abstractmethod
    def on_unreliable_stream_message(self, message, stream_id):
        pass

    @abstractmethod
    def on_file(self, filename, file_data):
        pass

    def _get_next_message_id(self):
        self._next_message_id += 1
        return self._next_message_id

    @staticmethod
    def _extract_packets(data):
        """Extract packets from a read chunk that starts with a barker (if there was something before - it was dropped).
        
        Note:
            The method assumes the data always starts with a barker

        Returns:
            list[str]: The extracted packets (the last packet might be only a part of a packet).
        """
        barker_indexes = [0]
        while True:
            barker_index = data.find(Protocol.BARKER, barker_indexes[-1] + Protocol.BARKER_LENGTH)
            if barker_index == -1:
                break

            barker_indexes.append(barker_index)
        
        packets = []
        for i in range(len(barker_indexes) - 1):
            packets.append(data[barker_indexes[i]:barker_indexes[i + 1]])

        packets.append(data[barker_indexes[-1]:]) # The rest might be a packet or a part of a packet
        return packets

    def _lock_file_stream(self):
        self._file_streams_lock.acquire()

        available_stream_id = None
        while available_stream_id is None:
            for stream_id in self.FILE_TRANSFER_STREAMS:
                if self._file_stream_id_to_is_available[stream_id]:
                    self._file_stream_id_to_is_available[stream_id] = False
                    available_stream_id = stream_id
                    break

        self._file_streams_lock.release()
        return available_stream_id

    def _handle_file_send(self, file_path):
        stream_id = self._lock_file_stream()

        is_last_chunk = b'0'
        filename = Protocol.str_to_bytes(os.path.basename(file_path))
        packet_index = 0
        self.send_reliable_stream_message(struct.pack('L', packet_index) + is_last_chunk + filename, stream_id)
        with open(file_path, 'rb') as transferred_file:
            chunk = transferred_file.read(self.CHUNK_SIZE)
            while chunk:
                packet_index += 1
                self.send_reliable_stream_message(struct.pack('L', packet_index) + is_last_chunk + chunk, stream_id)
                chunk = transferred_file.read(self.CHUNK_SIZE)

        is_last_chunk = b'1'
        packet_index += 1
        self.send_reliable_stream_message(struct.pack('L', packet_index) + is_last_chunk, stream_id)

        # Wait in order to make sure the file was received and processed at the other end (maybe adding a message that the transfer ended well is better)
        sleep(self.WAIT_BEFORE_FILE_STREAM_RELEASE)
        self._file_stream_id_to_is_available[stream_id] = True

    def _handle_file_chunks(self, stream_id, number_of_chunks):
        # Wait for all the chunks to arrive (they might have to be retransmitted)
        while len(self._file_stream_id_to_chunks[stream_id]) != number_of_chunks:
            sleep(0.1)

        filename = self._file_stream_id_to_chunks[stream_id][0].decode('utf8')

        file_data = bytearray()
        for i in range(1, number_of_chunks):
            file_data.extend(self._file_stream_id_to_chunks[stream_id][i])

        self._file_stream_id_to_chunks[stream_id] = {}
        self.on_file(filename, file_data)

    def _handle_unwrapped_message(self, message, message_id, stream_id, acked_message_id):
        # Check if ACK
        if acked_message_id:
            try:
                self._message_id_to_was_acked[acked_message_id] = True
            except KeyError:
                print('Got ACK on invalid message id {}'.format(acked_message_id))
            finally:
                return
        
        if message_id is None: # Unreliable
            if stream_id is None:
                self.on_unreliable_message(message)
            else:
                self.on_unreliable_stream_message(message, stream_id)

        else: # Reliable
            self._peer.send(Protocol.wrap_ack(message_id))
            if message_id not in self._messages_ids_that_have_been_received: # In order to avoid executing the same command twice due to retransmit
                self._messages_ids_that_have_been_received[message_id] = True # Could be anything (beside True). I just want to create the key

                if stream_id is None:
                    self.on_reliable_message(message)

                # Check if it's a file
                elif stream_id in self.FILE_TRANSFER_STREAMS:
                    chunk_index, = struct.unpack('L', message[:4])
                    # Check if it's the last chunk
                    is_last_chunk = message[4] # Unicode is returned
                    if is_last_chunk == 48: # ord('0')
                        is_last_chunk = False
                    elif is_last_chunk == 49: # ord('1')
                        is_last_chunk = True
                    else:
                        print('Error: Invalid unicode value for is_last_chunk: {}'.format(is_last_chunk))
                        return

                    if is_last_chunk:
                        Thread(target=self._handle_file_chunks, args=(stream_id, chunk_index)).start()
                    
                    else:
                        self._file_stream_id_to_chunks[stream_id][chunk_index] = message[5:] # 4 bytes of chunk_index and 1 byte of is_last_chunk

                else:
                    self.on_reliable_stream_message(message, stream_id)

    def _listen(self):
        data = b''
        while True:
            new_data = self._peer.receive(self.CHUNK_SIZE)
            if not new_data:
                continue

            data += new_data
            barker_index = data.find(Protocol.BARKER)
            if barker_index == -1: # Barker not found
                print('Bad data: {}'.format(data))
                data = data[-(Protocol.BARKER_LENGTH - 1):] # Drop the bad data and make sure not to drop the beginning of a barker
                continue

            data = data[barker_index:] # Drop everythinbg before the barker
            packets = self._extract_packets(data)

            for packet in packets[:-1]: # The last one might be only a part of a packet and should be 'data'
                try:
                    message, message_id, stream_id, acked_message_id, redundant_bytes = Protocol.unwrap(packet)
                    if redundant_bytes:
                        print('Redundant bytes: {}'.format(redundant_bytes))
                except ValueError:
                    print('Bad packet: {}'.format(packet))
                else:
                    self._handle_unwrapped_message(message, message_id, stream_id, acked_message_id)

            try:
                message, message_id, stream_id, acked_message_id, redundant_bytes = Protocol.unwrap(packets[-1])
            except ValueError:
                data = packets[-1] # This is probably only a part of a packet
            else:
                self._handle_unwrapped_message(message, message_id, stream_id, acked_message_id)
                data = redundant_bytes # The redundant bytes might be a part of another barker    

    def _send_message_and_wait_for_ack(self, message, message_id):
        self._peer.send(message)
        sleep(self.ACK_ARRIVAL_TIME)
        while not self._message_id_to_was_acked[message_id]:
            self._peer.send(message)
            sleep(self.ACK_ARRIVAL_TIME)

    def send_reliable(self, message):
        message_id = self._get_next_message_id()
        self._message_id_to_was_acked[message_id] = False
        message = Protocol.wrap_reliable(message, message_id)
        Thread(target=self._send_message_and_wait_for_ack(message, message_id)).start()

    def send_unreliable(self, message):
        self._peer.send(Protocol.wrap_unreliable(message))

    def send_reliable_stream_message(self, message, stream_id):
        message_id = self._get_next_message_id()
        self._message_id_to_was_acked[message_id] = False
        message = Protocol.wrap_reliable_stream(message, message_id, stream_id)
        Thread(target=self._send_message_and_wait_for_ack(message, message_id)).start()

    def send_unreliable_stream_message(self, message, stream_id):
        self._peer.send(Protocol.wrap_unreliable_stream(message, stream_id))

    def send_file(self, file_path):
        """Send a file over a reliable stream."""
        if not os.path.exists(file_path):
            raise ValueError('No such file: {}'.format(file_path))
        
        Thread(target=self._handle_file_send, args=(file_path,)).start()
