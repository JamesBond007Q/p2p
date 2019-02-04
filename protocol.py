import zlib
import struct


class Protocol(object):
    BARKER = b'BADFDADF'
    BEFORE_STUFF = b'BADFDAD'
    AFTER_STUFF = b'BADFDADZ'

    BARKER_LENGTH = len(BARKER) # Right after the barker

    @staticmethod
    def str_to_bytes(data):
        return bytearray(data, 'utf8')
    
    @staticmethod
    def _stuff_data(data):
        return data.replace(Protocol.BEFORE_STUFF, Protocol.AFTER_STUFF)
        
    @staticmethod
    def _unstuff_data(data):
        return data.replace(Protocol.AFTER_STUFF, Protocol.BEFORE_STUFF)

    @staticmethod
    def _extract_message(data, current_index):
        data_size, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        stuffed_data = data[current_index:current_index + data_size]
        current_index += data_size
        crc, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        redundant_bytes = data[current_index:]
            
        # check crc
        if crc != zlib.crc32(stuffed_data):
            raise ValueError('Invalid crc for packet: {}'.format(data))
            
        return Protocol._unstuff_data(stuffed_data), redundant_bytes

    @staticmethod
    def wrap_reliable(data, message_id):
        """Wrap the given data with reliable protocol so it can be sent."""
        if type(data) is str:
            data = Protocol.str_to_bytes(data)

        packet_type = chr(0).encode('utf8')
        message_id = struct.pack('L', message_id)
        stuffed_data = Protocol._stuff_data(data)
        data_size = struct.pack('L', len(stuffed_data))
        crc = struct.pack('L', zlib.crc32(stuffed_data))
        return Protocol.BARKER + packet_type + message_id + data_size + stuffed_data + crc

    @staticmethod
    def _unwrap_reliable(data):
        """Wrap the given data with reliable protocol so it can be sent."""
        current_index = Protocol.BARKER_LENGTH + 1
        message_id, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        message, redundant_bytes = Protocol._extract_message(data, current_index)
        return message, message_id, None, None, redundant_bytes

    @staticmethod
    def wrap_unreliable(data):
        """Wrap the given data with unreliable protocol so it can be sent."""
        if type(data) is str:
            data = Protocol.str_to_bytes(data)

        packet_type = chr(1).encode('utf8')
        stuffed_data = Protocol._stuff_data(data)
        data_size = struct.pack('L', len(stuffed_data))
        crc = struct.pack('L', zlib.crc32(stuffed_data))
        return Protocol.BARKER + packet_type + data_size + stuffed_data + crc

    @staticmethod
    def _unwrap_unreliable(data):
        message, redundant_bytes = Protocol._extract_message(data, Protocol.BARKER_LENGTH + 1)
        return message, None, None, None, redundant_bytes

    @staticmethod
    def wrap_reliable_stream(data, message_id, stream_id):
        """Wrap the given data with reliable protocol so it can be streamed."""
        if type(data) is str:
            data = Protocol.str_to_bytes(data)

        packet_type = chr(2).encode('utf8')
        message_id = struct.pack('L', message_id)
        stream_id = struct.pack('L', stream_id)
        stuffed_data = Protocol._stuff_data(data)
        data_size = struct.pack('L', len(stuffed_data))
        crc = struct.pack('L', zlib.crc32(stuffed_data))
        return Protocol.BARKER + packet_type + message_id + stream_id + data_size + stuffed_data + crc

    @staticmethod
    def _unwrap_reliable_stream(data):
        current_index = Protocol.BARKER_LENGTH + 1
        message_id, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        stream_id, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        message, redundant_bytes = Protocol._extract_message(data, current_index)
        return message, message_id, stream_id, None, redundant_bytes

    @staticmethod
    def wrap_unreliable_stream(data, stream_id):
        """Wrap the given data with unreliable protocol so it can be streamed."""
        if type(data) is str:
            data = Protocol.str_to_bytes(data)

        packet_type = chr(3).encode('utf8')
        stream_id = struct.pack('L', stream_id)
        stuffed_data = Protocol._stuff_data(data)
        data_size = struct.pack('L', len(stuffed_data))
        crc = struct.pack('L', zlib.crc32(stuffed_data))
        return Protocol.BARKER + packet_type + stream_id + data_size + stuffed_data + crc

    @staticmethod
    def _unwrap_unreliable_stream(data):
        current_index = Protocol.BARKER_LENGTH + 1
        stream_id, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        message, redundant_bytes = Protocol._extract_message(data, current_index)
        return message, None, stream_id, None, redundant_bytes

    @staticmethod
    def wrap_ack(acked_message_id):
        """Wrap the given data with unreliable protocol so it can be sent."""
        packet_type = chr(4).encode('utf8')
        acked_message_id = struct.pack('L', acked_message_id)
        return Protocol.BARKER + packet_type + acked_message_id

    @staticmethod
    def _unwrap_ack(data):
        current_index = Protocol.BARKER_LENGTH + 1
        acked_message_id, = struct.unpack('L', data[current_index:current_index + 4])
        current_index += 4
        redundant_bytes = data[current_index:]
        return None, None, None, acked_message_id, redundant_bytes

    @staticmethod
    def unwrap(data):
        """Unwrap the given data from the protocol layer so it can be received."""
        if type(data) is str:
            data = Protocol.str_to_bytes(data)

        try:
            # Validate barker
            if not data.startswith(Protocol.BARKER):
                raise ValueError('Packet is missing barker: {}'.format(data))

            # Get the packet type
            packet_type = data[Protocol.BARKER_LENGTH]
            return PACKET_TYPE_TO_UNWRAPPER[packet_type](data)
                
        except Exception as ex:
            raise ValueError('Invalid data: {}\nError was {}'.format(data, ex))


PACKET_TYPE_TO_UNWRAPPER = {
    0: Protocol._unwrap_reliable,
    1: Protocol._unwrap_unreliable,
    2: Protocol._unwrap_reliable_stream,
    3: Protocol._unwrap_unreliable_stream,
    4: Protocol._unwrap_ack,
}
