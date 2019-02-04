import os

from abstract_peer import AbstractPeer
from abstract_p2p_client import AbstractP2PClient


class DemoServerPeer(AbstractPeer):
    def __init__(self):
        if not os.path.exists('from_client'):
            with open('from_client', 'w') as f:
                pass

        self.input_stream = open('from_client', 'rb+')
        self.input_stream.read() # Move to the end of the stream to ignore old messages
        self.output_stream = open('from_server', 'ab+')

    def send(self, data):
        """Send bytes.
        
        Args:
            data (bytes): the bytes to send.
        """
        self.output_stream.write(data)
        self.output_stream.flush()

    def receive(self, receive_size):
        """Read bytes.

        Args:
            receive_size (number): the size to receive/read.
        
        Returns:
            bytes. the read bytes.
        """
        return self.input_stream.read(receive_size)


class DemoServer(AbstractP2PClient):
    def on_reliable_message(self, message):
        print('on_reliable_message: {}'.format(message))

    def on_unreliable_message(self, message):
        print('on_unreliable_message: {}'.format(message))

    def on_reliable_stream_message(self, message, stream_id):
        print('on_reliable_stream_message: {} {}'.format(stream_id, message))

    def on_unreliable_stream_message(self, message, stream_id):
        print('on_unreliable_stream_message: {} {}'.format(stream_id, message))

    def on_file(self, filename, file_data):
        print('on_file: {}'.format(filename))
        with open('s_transferred_' + filename, 'wb') as f:
            f.write(file_data)
